/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"sync"

	"github.com/minio/minio/pkg/quick"
)

// serverConfigV14New server configuration version '14' which is like
// version '13' except it adds support of browser param.
type serverConfigV14New struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential credential `json:"credential"`
	Region     string     `json:"region"`
	Browser    string     `json:"browser"`

	// Additional error logging configuration.
	Logger *logger `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
	mutex  *sync.RWMutex
}

// GetVersion get current config version.
func (s serverConfigV14New) GetVersion() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.Version
}

// SetRegion set new region.
func (s *serverConfigV14New) SetRegion(region string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV14New) GetRegion() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.Region
}

// SetCredentials set new credentials.
func (s *serverConfigV14New) SetCredential(creds credential) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Set updated credential.
	s.Credential = createCredential(creds.AccessKey, creds.SecretKey)
}

// GetCredentials get current credentials.
func (s serverConfigV14New) GetCredential() credential {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.Credential
}

// SetBrowser set if browser is enabled.
func (s *serverConfigV14New) SetBrowser(v string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Set browser param
	s.Browser = v
}

// GetCredentials get current credentials.
func (s serverConfigV14New) GetBrowser() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.Browser
}

// Save config.
func (s serverConfigV14New) Save(configFile string) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// initialize quick.
	qc, err := quick.New(&s)
	if err != nil {
		return err
	}

	// Save config file.
	return qc.Save(configFile)
}

func newServerConfigV14(cred credential, browser string) (srvCfg *serverConfigV14New) {
	srvCfg = &serverConfigV14New{
		Version: "14",
		Region:  "us-east-1",
		Logger:  &logger{},
		Notify:  &notifier{},
		mutex:   &sync.RWMutex{},
	}

	srvCfg.SetCredential(cred)
	srvCfg.SetBrowser(browser)

	// Enable console logger by default on a fresh run.
	srvCfg.Logger.Console = consoleLogger{
		Enable: true,
		Level:  "error",
	}

	// Make sure to initialize notification configs.
	srvCfg.Notify.AMQP = make(map[string]amqpNotify)
	srvCfg.Notify.AMQP["1"] = amqpNotify{}
	srvCfg.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
	srvCfg.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	srvCfg.Notify.Redis = make(map[string]redisNotify)
	srvCfg.Notify.Redis["1"] = redisNotify{}
	srvCfg.Notify.NATS = make(map[string]natsNotify)
	srvCfg.Notify.NATS["1"] = natsNotify{}
	srvCfg.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
	srvCfg.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	srvCfg.Notify.Kafka = make(map[string]kafkaNotify)
	srvCfg.Notify.Kafka["1"] = kafkaNotify{}
	srvCfg.Notify.Webhook = make(map[string]webhookNotify)
	srvCfg.Notify.Webhook["1"] = webhookNotify{}

	return srvCfg
}

func newServerConfig(cred credential, browser string) *serverConfigV14New {
	return newServerConfigV14(cred, browser)
}

func loadServerConfigV14(configFile string) (config *serverConfigV14New, err error) {
	config = &serverConfigV14New{Version: "14"}
	if err = loadOldConfig(configFile, config); err == nil {
		config.mutex = &sync.RWMutex{}
	}
	return config, err
}

func loadServerConfig(configFile string) (config *serverConfigV14New, err error) {
	return loadServerConfigV14(configFile)
}
