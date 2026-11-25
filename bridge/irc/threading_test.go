//nolint:goconst // Keep the IRC protocol fixtures readable in each test.
package birc

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lrstanley/girc"
	"github.com/matterbridge-org/matterbridge/bridge"
	"github.com/matterbridge-org/matterbridge/bridge/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const threadTestConfig = `
[irc.test]
Server="irc.example:6667"
Nick="bridgebot"
Charset="utf-8"
PreserveThreading=%t
MessageSplit=false
MessageDelay=1
`

func TestHandlePrivMsgDropsMalformedAndEchoEvents(t *testing.T) {
	b := &Birc{}
	for _, event := range []girc.Event{
		{},
		{Source: &girc.Source{Name: "alice"}, Params: []string{"#test"}},
		{Params: []string{"#test", "message"}},
		{Source: &girc.Source{Name: "bot"}, Params: []string{"#test", "echo"}, Echo: true},
	} {
		// These events must be discarded before accessing the client or gateway.
		b.handlePrivMsg(nil, event)
	}
}

func TestIRCThreadCorrelation(t *testing.T) {
	tracker := newIRCThreads()
	id := tracker.newID("#test")
	first, _ := tracker.begin(id, "#test", "PRIVMSG", "one")
	second, _ := tracker.begin(id, "#test", "PRIVMSG", "two")
	emit := func(raw string) { tracker.handleEvent("#test", *girc.ParseEvent(raw), true) }
	// A nested labeled response, with the second fragment echoed first.
	emit("@label=" + second + " :server BATCH +outer labeled-response")
	emit("@batch=outer :server BATCH +inner example")
	emit("@batch=inner;msgid=Second :bot!b@host PRIVMSG #test :two")
	require.Empty(t, tracker.resolve("#test", id, 0))
	emit("@label=" + first + ";msgid=First :bot!b@host PRIVMSG #test :one")
	require.Equal(t, "First", tracker.resolve("#test", id, 0))
	require.Empty(t, tracker.resolve("#other", id, 0))

	for _, parent := range []string{"First", "Second"} {
		_, got := tracker.receive("#test", *girc.ParseEvent("@+reply=" + parent + " :alice!a@host PRIVMSG #test :answer"))
		require.Equal(t, id, got)
	}

	_, parent := tracker.receive("#other", *girc.ParseEvent("@+reply=First :alice!a@host PRIVMSG #other :answer"))
	require.Equal(t, "First", parent)
	tracker.receive("#test", *girc.ParseEvent("@msgid=Native :alice!a@host PRIVMSG #test :original"))
	require.Equal(t, "Native", tracker.resolve("#test", "Native", 0))
	require.Empty(t, tracker.resolve("#test", "native", 0), "msgids are case sensitive")
	require.Empty(t, tracker.resolve("#test", "telegram 123", 0))
	_, parent = tracker.receive("#test", *girc.ParseEvent("@+reply=Native;+draft/reply=Old :alice!a@host PRIVMSG #test :answer"))
	require.Equal(t, "Native", parent)
	_, parent = tracker.receive("#test", *girc.ParseEvent("@+draft/reply=First :alice!a@host PRIVMSG #test :answer"))
	require.Equal(t, id, parent)
}

func TestIRCUnlabeledEchoAndExpiry(t *testing.T) {
	tracker := newIRCThreads()
	id := tracker.newID("#test")
	label, _ := tracker.begin(id, "#test", "PRIVMSG", "same text")
	tracker.handleEvent("#test", *girc.ParseEvent("@label=unknown;msgid=wrong :bot!b@h PRIVMSG #test :same text"), true)
	tracker.handleEvent("#test", *girc.ParseEvent("@msgid=other-user :alice!a@h PRIVMSG #test :same text"), false)
	require.Empty(t, tracker.resolve("#test", id, 0))
	tracker.handleEvent("#test", *girc.ParseEvent("@msgid=correct :bot!b@h PRIVMSG #test :same text"), true)
	require.Equal(t, "correct", tracker.resolve("#test", id, 0))
	require.False(t, tracker.pending.Contains(label))

	id = tracker.newID("#test")
	label, _ = tracker.begin(id, "#test", "PRIVMSG", "same text")
	p, _ := tracker.pending.Get(label)
	p.created = time.Now().Add(-2 * echoLifetime)
	tracker.pending.Add(label, p)
	tracker.handleEvent("#test", *girc.ParseEvent("@msgid=too-late :bot!b@h PRIVMSG #test :same text"), true)
	require.Empty(t, tracker.resolve("#test", id, 0))
}

func TestIRCDisconnectReleasesEchoWait(t *testing.T) {
	tracker := newIRCThreads()
	id := tracker.newID("#test")

	finished := make(chan string, 1)
	go func() { finished <- tracker.resolve("#test", id, time.Minute) }()

	tracker.disconnected()

	select {
	case got := <-finished:
		require.Empty(t, got)
	case <-time.After(time.Second):
		t.Fatal("disconnect did not release pending echo wait")
	}
}

func TestIRCSplitFrames(t *testing.T) {
	for _, raw := range []string{
		"PRIVMSG #test :" + strings.Repeat("слово🙂 ", 50),
		"NOTICE #test :" + strings.Repeat("a", 500),
		"PRIVMSG #test :\x01ACTION " + strings.Repeat("🙂", 50) + "\x01",
		"RELAYMSG #test alice :\x01ACTION " + strings.Repeat("🙂", 50) + "\x01",
	} {
		event := girc.ParseEvent(raw)
		parts := splitIRCEvent(event, 80)
		require.Greater(t, len(parts), 1)

		var reconstructed strings.Builder

		for _, part := range parts {
			require.LessOrEqual(t, part.LenOpts(false), 80)
			require.True(t, utf8.ValidString(part.Last()))

			text := part.Last()
			if strings.HasPrefix(event.Last(), "\x01ACTION ") {
				require.True(t, strings.HasPrefix(text, "\x01ACTION "))
				require.True(t, strings.HasSuffix(text, "\x01"))
				text = strings.TrimSuffix(strings.TrimPrefix(text, "\x01ACTION "), "\x01")
			}

			reconstructed.WriteString(text)
		}

		original := strings.TrimSuffix(strings.TrimPrefix(event.Last(), "\x01ACTION "), "\x01")
		require.Equal(t, original, reconstructed.String())
	}
}

// Uses the real girc connection, CAP negotiation, serializer and echo dispatch.
// No public IRC or Telegram/Discord credentials are required.
func newThreadTestConnection(t *testing.T, caps string, enabled bool) (*Birc, func(string), func() *girc.Event) {
	t.Helper()

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	cfg := config.NewConfigFromString(logger, fmt.Appendf(nil, threadTestConfig, enabled))
	br := bridge.New(&config.Bridge{Account: "irc.test"})
	br.Config, br.General, br.Log = cfg, &cfg.BridgeValues().General, logrus.NewEntry(logger)
	b, ok := New(&bridge.Config{Bridge: br, Remote: make(chan config.Message, 20)}).(*Birc)
	require.True(t, ok)

	b.prefixDone, b.authDone = true, true
	b.MessagePrefix = 80
	b.Local = make(chan config.Message, 40)
	client, err := b.getClient()
	require.NoError(t, err)

	b.i = client
	client.Handlers.Add(girc.ALL_EVENTS, b.handleThreadEvent)
	client.Handlers.Add(girc.PRIVMSG, b.handlePrivMsg)
	client.Handlers.Add(girc.NOTICE, b.handlePrivMsg)

	peer, socket := net.Pipe()
	lines := make(chan *girc.Event, 100)

	done := make(chan struct{})
	go func() { defer close(done); _ = client.MockConnect(socket) }()
	go serveThreadTestIRC(peer, caps, lines)

	t.Cleanup(func() {
		_ = peer.Close()
		client.Close()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("mock IRC connection did not stop")
		}
	})
	require.Eventually(t, func() bool { return client.HasCapability("server-time") }, 3*time.Second, time.Millisecond)

	send := func(raw string) {
		t.Helper()
		require.NoError(t, peer.SetWriteDeadline(time.Now().Add(3*time.Second)))
		_, err := fmt.Fprint(peer, raw+"\r\n")
		require.NoError(t, err)
	}
	recv := func() *girc.Event {
		t.Helper()

		select {
		case e := <-lines:
			require.NotNil(t, e)
			return e
		case <-time.After(3 * time.Second):
			t.Fatal("no IRC message sent")
			return nil
		}
	}

	return b, send, recv
}

func TestIRCNativeRepliesOnWire(t *testing.T) { //nolint:funlen // Exercises a complete reply round trip and split-message replies.
	b, fromServer, onWire := newThreadTestConnection(t, "server-time message-tags echo-message labeled-response batch", true)
	send := func(msg config.Message) (string, *girc.Event) {
		t.Helper()

		msg.Channel, msg.Username = "#test", "<alice> "
		id, err := b.Send(msg)
		require.NoError(t, err)
		require.NotEmpty(t, id)

		queued := <-b.Local
		require.NoError(t, b.sendIRC(queued, "PRIVMSG #test :"+queued.Username+queued.Text))

		return id, onWire()
	}
	id, original := send(config.Message{Text: "original", Protocol: "telegram"})
	label, ok := original.Tags.Get("label")
	require.True(t, ok)
	fromServer("@label=" + label + ";msgid=OriginalID :bridgebot!b@host PRIVMSG #test :<alice> original")
	require.Equal(t, "OriginalID", b.threads.resolve("#test", id, time.Second))

	for _, protocol := range []string{"telegram", "discord"} {
		_, reply := send(config.Message{Text: "reply", Protocol: protocol, ParentID: id})
		parent, _ := reply.Tags.Get("+reply")
		require.Equal(t, "OriginalID", parent, protocol)

		legacy, _ := reply.Tags.Get("+draft/reply")
		require.Equal(t, parent, legacy)
	}

	select {
	case msg := <-b.Remote:
		t.Fatalf("own echo looped to gateway: %#v", msg)
	default:
	}

	fromServer("@msgid=NativeReply;+reply=OriginalID :bob!b@host PRIVMSG #test :native answer")

	select {
	case msg := <-b.Remote:
		require.Equal(t, "NativeReply", msg.ID)
		require.Equal(t, id, msg.ParentID)
	case <-time.After(time.Second):
		t.Fatal("native reply not delivered to gateway")
	}

	_, reply := send(config.Message{Text: "answer to native IRC message", ParentID: "NativeReply"})
	parent, _ := reply.Tags.Get("+reply")
	require.Equal(t, "NativeReply", parent)

	_, reply = send(config.Message{Text: "(re @bob: old message) fallback", ParentID: "unknown"})
	_, hasReply := reply.Tags.Get("+reply")
	require.False(t, hasReply)
	require.Contains(t, reply.Last(), "(re @bob: old message) fallback")

	// Every split wire frame gets a distinct label; only the first is a
	// reply, but replying to the final fragment still reaches the source.
	b.MessageLength = 180
	longID, err := b.Send(config.Message{Channel: "#test", Text: strings.Repeat("🙂", 80), ParentID: "NativeReply"})
	require.NoError(t, err)

	queued := <-b.Local
	parts := splitIRCEvent(girc.ParseEvent("PRIVMSG #test :"+queued.Text), b.MessageLength-b.MessagePrefix)
	require.NoError(t, b.sendIRC(queued, "PRIVMSG #test :"+queued.Text))

	labels := make(map[string]bool)

	for index := range parts {
		frame := onWire()
		frameLabel, _ := frame.Tags.Get("label")
		require.NotEmpty(t, frameLabel)
		require.False(t, labels[frameLabel])
		labels[frameLabel] = true

		gotParent, _ := frame.Tags.Get("+reply")
		if index == 0 {
			require.Equal(t, "NativeReply", gotParent)
		} else {
			require.Empty(t, gotParent)
		}

		fromServer(fmt.Sprintf("@label=%s;msgid=part%d :bridgebot!b@host PRIVMSG #test :%s", frameLabel, index, frame.Last()))
	}

	require.Equal(t, "part0", b.threads.resolve("#test", longID, time.Second))
	fromServer(fmt.Sprintf("@msgid=reply-to-last;+reply=part%d :bob!b@host PRIVMSG #test :answer to last fragment", len(parts)-1))

	select {
	case msg := <-b.Remote:
		require.Equal(t, longID, msg.ParentID)
	case <-time.After(time.Second):
		t.Fatal("reply to final fragment not delivered")
	}
}

func TestIRCSplitQueueKeepsIDForSentFragments(t *testing.T) {
	b, _, _ := newThreadTestConnection(t, "server-time", true)
	b.SetBool("MessageSplit", true)
	b.MessageQueue = 1

	id, err := b.Send(config.Message{Channel: "#test", Text: "first\nsecond\nthird"})
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.Len(t, b.Local, 1)

	droppedID, err := b.Send(config.Message{Channel: "#test", Text: "queue is full"})
	require.NoError(t, err)
	require.Empty(t, droppedID)

	queued := <-b.Local
	require.Equal(t, "first", queued.Text)
	require.Equal(t, id, queued.ID)
}

func TestIRCRelayMessageOnWire(t *testing.T) {
	b, _, onWire := newThreadTestConnection(t, "server-time message-tags echo-message draft/relaymsg", true)
	b.SetBool("UseRelayMsg", true)
	b.SetBool("DoubleColonPrefix", true)

	msg := config.Message{Channel: "#test", Username: "alice", Text: ":word"}
	msg.ID = b.threads.newID(msg.Channel)
	require.True(t, b.sendRelayMessage(msg))

	frame := onWire()
	require.Equal(t, "RELAYMSG", frame.Command)
	require.Equal(t, []string{"#test", "alice", "::word"}, frame.Params)

	msg.Event, msg.Text = config.EventUserAction, "waves"
	msg.ID = b.threads.newID(msg.Channel)
	require.True(t, b.sendRelayMessage(msg))
	require.Equal(t, "\x01ACTION waves\x01", onWire().Last())

	b.SetBool("UseRelayMsg", false)
	require.False(t, b.sendRelayMessage(msg))
}

func TestIRCThreadingFallbackOnWire(t *testing.T) {
	for _, tc := range []struct {
		name, caps string
		enabled    bool
	}{
		{"legacy server", "server-time", true},
		{"disabled", "server-time message-tags echo-message labeled-response batch", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _, onWire := newThreadTestConnection(t, tc.caps, tc.enabled)
			msg := config.Message{Channel: "#test", Text: "(re @alice: original) reply", ParentID: "unknown"}
			require.NoError(t, b.sendIRC(msg, "PRIVMSG #test :"+msg.Text))

			e := onWire()
			require.Equal(t, msg.Text, e.Last())
			require.Empty(t, e.Tags)
		})
	}
}

func serveThreadTestIRC(peer net.Conn, caps string, lines chan<- *girc.Event) {
	defer close(lines)

	scanner := bufio.NewScanner(peer)
	for scanner.Scan() {
		e := girc.ParseEvent(scanner.Text())
		if e == nil {
			continue
		}

		switch {
		case e.Command == "CAP" && e.Params[0] == "LS":
			_, _ = fmt.Fprintf(peer, ":server CAP bridgebot LS :%s\r\n", caps)
		case e.Command == "CAP" && e.Params[0] == "REQ":
			_, _ = fmt.Fprintf(peer, ":server CAP bridgebot ACK :%s\r\n", e.Last())
		case e.Command == "CAP" && e.Params[0] == "END":
			_, _ = fmt.Fprint(peer, ":server 001 bridgebot :Welcome\r\n")
		case e.Command == "PRIVMSG" || e.Command == "NOTICE" || e.Command == "RELAYMSG":
			lines <- e
		}
	}
}
