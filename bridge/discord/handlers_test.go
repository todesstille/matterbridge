package bdiscord

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/matterbridge-org/matterbridge/bridge"
	"github.com/matterbridge-org/matterbridge/bridge/config"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func handleEmbed(embed *discordgo.MessageEmbed) string {
	var result string

	result = " embed: {TITLE} - {DESCRIPTION} - {URL}"

	result = strings.ReplaceAll(result, "{URL}", embed.URL)
	result = strings.ReplaceAll(result, "{TITLE}", embed.Title)
	result = strings.ReplaceAll(result, "{DESCRIPTION}", embed.Description)

	if result != "" {
		result += "\n"
	}

	return result
}

func TestHandleEmbed(t *testing.T) {
	testcases := map[string]struct {
		embed  *discordgo.MessageEmbed
		result string
	}{
		"allempty": {
			embed:  &discordgo.MessageEmbed{},
			result: " embed:  -  - \n",
		},
		"one": {
			embed: &discordgo.MessageEmbed{
				Title: "blah",
			},
			result: " embed: blah -  - \n",
		},
		"two": {
			embed: &discordgo.MessageEmbed{
				Title:       "blah",
				Description: "blah2",
			},
			result: " embed: blah - blah2 - \n",
		},
		"three": {
			embed: &discordgo.MessageEmbed{
				Title:       "blah",
				Description: "blah2",
				URL:         "blah3",
			},
			result: " embed: blah - blah2 - blah3\n",
		},
		"twob": {
			embed: &discordgo.MessageEmbed{
				Description: "blah2",
				URL:         "blah3",
			},
			result: " embed:  - blah2 - blah3\n",
		},
		"oneb": {
			embed: &discordgo.MessageEmbed{
				URL: "blah3",
			},
			result: " embed:  -  - blah3\n",
		},
	}

	for name, tc := range testcases {
		assert.Equalf(t, tc.result, handleEmbed(tc.embed), "Testcases %s", name)
	}
}

const testFileContents = "file contents"

// testConfig is a minimal config.Config implementation for unit tests.
type testConfig struct {
	values map[string]any
}

func (c *testConfig) Viper() *viper.Viper                { return nil }
func (c *testConfig) BridgeValues() *config.BridgeValues { return &config.BridgeValues{} }
func (c *testConfig) IsKeySet(key string) bool           { _, ok := c.values[key]; return ok }

func (c *testConfig) GetBool(key string) (bool, bool) {
	v, ok := c.values[key].(bool)
	return v, ok
}

func (c *testConfig) GetInt(key string) (int, bool) {
	v, ok := c.values[key].(int)
	return v, ok
}

func (c *testConfig) GetString(key string) (string, bool) {
	v, ok := c.values[key].(string)
	return v, ok
}

func (c *testConfig) GetStringSlice(key string) ([]string, bool) {
	v, ok := c.values[key].([]string)
	return v, ok
}

func (c *testConfig) GetStringSlice2D(key string) ([][]string, bool) {
	v, ok := c.values[key].([][]string)
	return v, ok
}

func (c *testConfig) IsFilenameBlacklisted(string) bool { return false }
func (c *testConfig) SetVal(string, any)                {}

// testBridger is a minimal bridge.Bridger implementation for unit tests.
type testBridger struct{}

func (testBridger) Send(config.Message) (string, error)        { return "", nil }
func (testBridger) Connect() error                             { return nil }
func (testBridger) JoinChannel(config.ChannelInfo) error       { return nil }
func (testBridger) Disconnect() error                          { return nil }
func (testBridger) SanitizeNick(*config.Message) error         { return nil }
func (testBridger) NewHttpClient(string) (*http.Client, error) { return http.DefaultClient, nil }
func (testBridger) NewHttpRequest(method, uri string, body io.Reader) (*http.Request, error) {
	return http.NewRequest(method, uri, body)
}

func newTestDiscord(t *testing.T, client *http.Client) *Bdiscord {
	t.Helper()

	return &Bdiscord{
		Config: &bridge.Config{
			Bridge: &bridge.Bridge{
				Name:       "test",
				Account:    "discord.test",
				Protocol:   "discord",
				Config:     &testConfig{values: map[string]any{"general.UseUserName": true}},
				Log:        logrus.NewEntry(logrus.New()),
				General:    &config.Protocol{MediaDownloadSize: 1000000},
				HttpClient: client,
				Bridger:    testBridger{},
			},
			Remote: make(chan config.Message, 1),
		},
		c:              &discordgo.Session{},
		guildID:        "guild-id",
		nick:           "matterbridge",
		userMemberMap:  make(map[string]*discordgo.Member),
		nickMemberMap:  make(map[string]*discordgo.Member),
		channelInfoMap: make(map[string]*config.ChannelInfo),
		channels:       []*discordgo.Channel{{ID: "channel-id", Name: "test"}},
	}
}

func TestMessageCreateAttachmentComments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(testFileContents))
	}))
	defer server.Close()

	testcases := map[string]struct {
		content string
		// comments expected on the two attachments
		comments []string
		text     string
	}{
		"text with attachments": {
			content:  "look at this",
			comments: []string{"look at this", ""},
			text:     "",
		},
		"attachments without text": {
			content:  "",
			comments: []string{"", ""},
			text:     "",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			b := newTestDiscord(t, server.Client())

			b.messageCreate(b.c, &discordgo.MessageCreate{Message: &discordgo.Message{
				GuildID:   b.guildID,
				ChannelID: "channel-id",
				ID:        "message-id",
				Content:   tc.content,
				Author:    &discordgo.User{ID: "user-id", Username: "alice"},
				Attachments: []*discordgo.MessageAttachment{
					{ID: "att-1", Filename: "first.jpg", URL: server.URL + "/first.jpg"},
					{ID: "att-2", Filename: "second.jpg", URL: server.URL + "/second.jpg"},
				},
			}})

			select {
			case rmsg := <-b.Remote:
				require.Len(t, rmsg.Extra["file"], 2)
				for i, want := range tc.comments {
					fi, ok := rmsg.Extra["file"][i].(config.FileInfo)
					require.True(t, ok)
					assert.Equalf(t, want, fi.Comment, "comment for attachment %d", i)
				}
				assert.Equal(t, tc.text, rmsg.Text)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for message")
			}
		})
	}
}
