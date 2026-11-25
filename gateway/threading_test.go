//nolint:goconst // Keep protocol names and message fixtures explicit.
package gateway

import (
	"testing"

	"github.com/matterbridge-org/matterbridge/bridge"
	"github.com/matterbridge-org/matterbridge/bridge/config"
	"github.com/stretchr/testify/require"
)

type threadCaptureBridge struct {
	bridge.Bridger

	message config.Message
}

func (b *threadCaptureBridge) Send(msg config.Message) (string, error) {
	b.message = msg
	return "sent", nil
}

func TestIRCReplyGatewayMapping(t *testing.T) { //nolint:funlen // Covers both directions and missing destination IDs.
	router := maketestRouter([]byte(`
[general]
PreserveThreading=true
[irc.test]
Server=""
[telegram.test]
Token=""
[discord.test]
Token=""
[[gateway]]
name="test"
enable=true
[[gateway.inout]]
account="irc.test"
channel="#test"
[[gateway.inout]]
account="telegram.test"
channel="123"
[[gateway.inout]]
account="discord.test"
channel="456"
`))
	gw := router.Gateways["test"]
	irc := gw.Bridges["irc.test"]
	ircChannel := gw.Channels["#testirc.test"]
	ircCapture := &threadCaptureBridge{Bridger: irc.Bridger}
	irc.Bridger = ircCapture

	for _, protocol := range []string{"telegram", "discord"} {
		t.Run(protocol, func(t *testing.T) {
			source := gw.Bridges[protocol+".test"]

			var sourceChannel *config.ChannelInfo

			for _, ch := range gw.Channels {
				if ch.Account == source.Account {
					sourceChannel = ch
				}
			}

			require.NotNil(t, sourceChannel)

			canonical := protocol + " source-id"
			gw.Messages.Add(canonical, []*BrMsgID{{br: irc, ID: "irc local-id", ChannelID: ircChannel.ID}})

			reply := &config.Message{Account: source.Account, Protocol: protocol, Channel: sourceChannel.Name, Username: "alice", Text: "reply", ParentID: "source-id"}
			parent := gw.FindCanonicalMsgID(protocol, reply.ParentID)
			_, err := gw.SendMessage(reply, irc, ircChannel, parent)
			require.NoError(t, err)
			require.Equal(t, "local-id", ircCapture.message.ParentID)

			// A native IRC reply to the echoed message returns to its origin.
			capture := &threadCaptureBridge{Bridger: source.Bridger}
			source.Bridger = capture
			parent = gw.FindCanonicalMsgID("irc", "local-id")
			nativeReply := &config.Message{Account: irc.Account, Protocol: "irc", Channel: "#test", Username: "bob", Text: "answer", ParentID: "local-id"}
			_, err = gw.SendMessage(nativeReply, source, sourceChannel, parent)
			require.NoError(t, err)
			require.Equal(t, "source-id", capture.message.ParentID)

			// Parents without an IRC destination (or sent to another channel)
			// must not leak a foreign protocol ID into IRC reply tags.
			for _, ids := range [][]*BrMsgID{
				nil,
				{{br: irc, ID: "irc elsewhere", ChannelID: "#otherirc.test"}},
			} {
				gw.Messages.Add(canonical, ids)
				_, err = gw.SendMessage(reply, irc, ircChannel, canonical)
				require.NoError(t, err)
				require.Equal(t, config.ParentIDNotFound, ircCapture.message.ParentID)
			}

			gw.Messages.Remove(canonical)
		})
	}
}
