package birc

import (
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/lrstanley/girc"
	"github.com/matterbridge-org/matterbridge/bridge/config"
)

const (
	threadCacheSize = 5000
	echoCacheSize   = 200
	echoLifetime    = 30 * time.Second
	echoWait        = 2 * time.Second
)

var (
	errInvalidIRCMessage = errors.New("invalid IRC message")
	errIRCLineBudget     = errors.New("IRC message has no body or exceeds the line budget")
)

type ircMessageKey struct{ channel, id string }

type outgoingMessage struct {
	channel, serverID string
	ready             chan struct{}
	sent, done        bool
}

func (m *outgoingMessage) finish() {
	if !m.done {
		close(m.ready)
		m.done = true
	}
}

type pendingEcho struct {
	localID, channel, command, text string
	first                           bool
	created                         time.Time
}

type labeledBatch struct {
	label   string
	created time.Time
}

// IRC supplies IDs only after sending. The gateway gets a local ID immediately;
// echo-message subsequently binds it to the server's opaque, case-sensitive ID.
// All caches are bounded, scoped to this connection's network and to a channel.
type ircThreads struct {
	mu       sync.Mutex
	outgoing *simplelru.LRU[string, *outgoingMessage]
	server   *simplelru.LRU[ircMessageKey, string]
	pending  *simplelru.LRU[string, pendingEcho]
	batches  *simplelru.LRU[string, labeledBatch]
}

func newIRCThreads() *ircThreads {
	t := &ircThreads{}
	t.outgoing, _ = simplelru.NewLRU(threadCacheSize, func(_ string, m *outgoingMessage) { m.finish() })
	t.server, _ = simplelru.NewLRU[ircMessageKey, string](threadCacheSize, nil)
	t.pending, _ = simplelru.NewLRU[string, pendingEcho](echoCacheSize, nil)
	t.batches, _ = simplelru.NewLRU[string, labeledBatch](echoCacheSize, nil)

	return t
}

func (t *ircThreads) newID(channel string) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	id := "mb-" + rand.Text()
	t.outgoing.Add(id, &outgoingMessage{channel: channel, ready: make(chan struct{})})

	return id
}

func (t *ircThreads) resolve(channel, id string, wait time.Duration) string {
	t.mu.Lock()

	m, ok := t.outgoing.Get(id)
	if !ok {
		_, known := t.server.Get(ircMessageKey{channel, id})
		t.mu.Unlock()

		if known {
			return id
		}

		return ""
	}

	if m.channel != channel {
		t.mu.Unlock()
		return ""
	}

	ready := m.ready
	t.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ready:
	case <-timer.C:
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return m.serverID
}

func (t *ircThreads) begin(id, channel, command, text string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	m, ok := t.outgoing.Get(id)
	if !ok || m.channel != channel {
		return "", false
	}

	first := !m.sent
	m.sent = true
	label := "mb-" + rand.Text()
	t.pending.Add(label, pendingEcho{id, channel, command, text, first, time.Now()})

	return label, first
}

func (t *ircThreads) receive(channel string, event girc.Event) (string, string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	id, _ := event.Tags.Get("msgid")
	if id != "" {
		if !t.server.Contains(ircMessageKey{channel, id}) {
			t.server.Add(ircMessageKey{channel, id}, "")
		}
	}

	parent, _ := event.Tags.Get("+reply")
	if parent == "" {
		parent, _ = event.Tags.Get("+draft/reply")
	}

	if localID, ok := t.server.Get(ircMessageKey{channel, parent}); ok && localID != "" {
		parent = localID
	}

	return id, parent
}

// Labels can be on the message itself or inherited from a labeled-response
// BATCH (including nested batches). Unknown labels must not match by text.
func (t *ircThreads) handleEvent(channel string, event girc.Event, own bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	label, message := t.eventLabel(event, now)
	if !message {
		return
	}

	id, _ := event.Tags.Get("msgid")
	if !own || id == "" || len(event.Params) < 2 || (event.Command != girc.PRIVMSG && event.Command != girc.NOTICE) {
		return
	}

	label, pending, found := t.matchEcho(label, channel, event, now)
	if !found {
		return
	}

	t.pending.Remove(label)

	if m, ok := t.outgoing.Get(pending.localID); ok {
		// The first transmitted fragment is the anchor even if echoes arrive
		// out of order. Replies to any fragment map back to the same source.
		if pending.first && !m.done {
			m.serverID = id
			m.finish()
		}

		t.server.Add(ircMessageKey{channel, id}, pending.localID)
	}
}

// eventLabel is called with mu held.
func (t *ircThreads) eventLabel(event girc.Event, now time.Time) (string, bool) {
	label, _ := event.Tags.Get("label")
	if label == "" {
		batch, _ := event.Tags.Get("batch")
		if entry, ok := t.batches.Get(batch); ok && now.Sub(entry.created) < echoLifetime {
			label = entry.label
		}
	}

	if event.Command == "BATCH" && len(event.Params) != 0 {
		if id, ok := strings.CutPrefix(event.Params[0], "+"); ok && label != "" {
			t.batches.Add(id, labeledBatch{label, now})
		} else if id, ok := strings.CutPrefix(event.Params[0], "-"); ok {
			t.batches.Remove(id)
		}

		return "", false
	}

	if event.Command == "ACK" {
		t.pending.Remove(label)
		return "", false
	}

	return label, true
}

// matchEcho is called with mu held.
func (t *ircThreads) matchEcho(label, channel string, event girc.Event, now time.Time) (string, pendingEcho, bool) {
	if label != "" {
		pending, found := t.pending.Get(label)
		return label, pending, found && now.Sub(pending.created) < echoLifetime && pending.channel == channel
	}
	// Without labeled-response, use the oldest exact matching own echo.
	for _, key := range t.pending.Keys() {
		p, _ := t.pending.Peek(key)
		if now.Sub(p.created) < echoLifetime && p.channel == channel && p.command == event.Command && p.text == event.Last() {
			return key, p, true
		}
	}

	return "", pendingEcho{}, false
}

func (t *ircThreads) disconnected() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pending.Purge()
	t.batches.Purge()

	for _, id := range t.outgoing.Keys() {
		m, _ := t.outgoing.Peek(id)
		m.finish()
	}
}

func (b *Birc) threadChannel(channel string) string {
	// Match the channel names used by handlePrivMsg and the gateway. Do not
	// apply RFC1459 punctuation folding to networks with ASCII casemapping.
	return strings.ToLower(channel)
}

func (b *Birc) handleThreadEvent(client *girc.Client, event girc.Event) {
	channel := ""
	if len(event.Params) != 0 {
		channel = b.threadChannel(event.Params[0])
	}

	own := event.Echo || b.isOwnMessage(client, event)
	b.threads.handleEvent(channel, event, own)
}

func (b *Birc) isOwnMessage(client *girc.Client, event girc.Event) bool {
	nick := client.GetNick()
	if event.Source != nil && strings.EqualFold(event.Source.Name, nick) {
		return true
	}

	for _, tag := range []string{"draft/relaymsg", "relaymsg"} {
		if relayer, ok := event.Tags.Get(tag); ok && strings.EqualFold(relayer, nick) {
			return true
		}
	}

	return false
}

// Split before labeling: each wire command needs a distinct label. Letting
// girc split a labeled event would reuse its label and break echo correlation.
// Tags have a separate IRCv3 size allowance and do not consume the body budget.
func splitIRCEvent(event *girc.Event, limit int) []*girc.Event {
	text := event.Last()

	prefix, suffix := "", ""
	if event.IsAction() || strings.HasPrefix(text, "\x01ACTION ") && strings.HasSuffix(text, "\x01") {
		prefix, suffix = "\x01ACTION ", "\x01"
		text = strings.TrimSuffix(strings.TrimPrefix(text, prefix), suffix)
	}

	budget := limit - (event.LenOpts(false) - len(event.Last())) - len(prefix) - len(suffix)
	if budget < utf8.UTFMax {
		return nil
	}

	var events []*girc.Event

	for line := range strings.SplitSeq(strings.ReplaceAll(text, "\r", ""), "\n") {
		for len(line) > 0 {
			n := min(len(line), budget)
			if n < len(line) {
				for n > 0 && !utf8.RuneStart(line[n]) {
					n--
				}
			}

			if n == 0 {
				return nil
			}

			part := event.Copy()
			part.Params[len(part.Params)-1] = prefix + line[:n] + suffix
			events = append(events, part)
			line = line[n:]
		}
	}

	return events
}

func (b *Birc) sendIRC(msg config.Message, raw string) error {
	if !b.GetBool("PreserveThreading") || !b.i.HasCapability("message-tags") {
		if b.GetBool("MessageSplit") {
			return b.i.Cmd.SendRawNoSplit(raw + "\r\n")
		}

		return b.i.Cmd.SendRaw(raw)
	}

	event := girc.ParseEvent(raw)
	if event == nil {
		return errInvalidIRCMessage
	}

	parts := splitIRCEvent(event, b.MessageLength-b.MessagePrefix)
	if len(parts) == 0 {
		return errIRCLineBudget
	}

	channel := b.threadChannel(msg.Channel)
	parent := ""

	if msg.ParentValid() {
		wait := time.Duration(0)
		if b.i.HasCapability("echo-message") {
			wait = echoWait
		}

		parent = b.threads.resolve(channel, msg.ParentID, wait)
	}

	for index, part := range parts {
		if index > 0 {
			// These extra fragments bypass girc's splitting and rate limiter.
			time.Sleep(time.Duration(b.MessageDelay) * time.Millisecond)
		}

		b.addThreadTags(msg, channel, parent, part)

		err := b.i.Cmd.SendRawNoSplit(part.String() + "\r\n")
		if err != nil {
			return err
		}
	}

	return nil
}

// addThreadTags uses fixed valid tag names. If an oversized server ID exceeds
// girc's tag budget, Set omits that tag and the text is still sent.
func (b *Birc) addThreadTags(msg config.Message, channel, parent string, part *girc.Event) {
	command := part.Command
	if command == "RELAYMSG" {
		command = girc.PRIVMSG // RELAYMSG is echoed as PRIVMSG.
	}

	label, first := b.threads.begin(msg.ID, channel, command, part.Last())

	part.Tags = make(girc.Tags)
	if first && parent != "" {
		_ = part.Tags.Set("+reply", parent)
		_ = part.Tags.Set("+draft/reply", parent) // Older IRC clients.
	}

	if label != "" && b.i.HasCapability("echo-message") && b.i.HasCapability("labeled-response") {
		_ = part.Tags.Set("label", label)
	}
}
