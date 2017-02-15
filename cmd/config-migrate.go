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
	"fmt"
	"os"
	"path/filepath"

	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/quick"
)

func migrateConfig(configFile string) error {
	// Purge all configs with version '1'.
	if err := purgeV1(configFile); err != nil {
		return err
	}
	// Migrate version '2' to '3'.
	if err := migrateV2ToV3(configFile); err != nil {
		return err
	}
	// Migrate version '3' to '4'.
	if err := migrateV3ToV4(configFile); err != nil {
		return err
	}
	// Migrate version '4' to '5'.
	if err := migrateV4ToV5(configFile); err != nil {
		return err
	}
	// Migrate version '5' to '6.
	if err := migrateV5ToV6(configFile); err != nil {
		return err
	}
	// Migrate version '6' to '7'.
	if err := migrateV6ToV7(configFile); err != nil {
		return err
	}
	// Migrate version '7' to '8'.
	if err := migrateV7ToV8(configFile); err != nil {
		return err
	}
	// Migrate version '8' to '9'.
	if err := migrateV8ToV9(configFile); err != nil {
		return err
	}
	// Migrate version '9' to '10'.
	if err := migrateV9ToV10(configFile); err != nil {
		return err
	}
	// Migrate version '10' to '11'.
	if err := migrateV10ToV11(configFile); err != nil {
		return err
	}
	// Migrate version '11' to '12'.
	if err := migrateV11ToV12(configFile); err != nil {
		return err
	}
	// Migration version '12' to '13'.
	if err := migrateV12ToV13(configFile); err != nil {
		return err
	}
	// Migration version '13' to '14'.
	if err := migrateV13ToV14(configFile); err != nil {
		return err
	}

	return nil
}

// Version '1' is deprecated and not supported anymore, safe to delete.
func purgeV1(configFile string) error {
	cv1, err := loadConfigV1(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘1’. %v", err)

	}

	if cv1.Version != "1" {
		return fmt.Errorf("Unrecognized config version ‘" + cv1.Version + "’.")
	}

	configFile = filepath.Join(filepath.Dir(configFile), "fsUsers.json")
	removeAll(configFile)
	console.Println("Removed unsupported config version ‘1’.")
	return nil
}

// Version '2' to '3' config migration adds new fields and re-orders
// previous fields. Simplifies config for future additions.
func migrateV2ToV3(configFile string) error {
	cv2, err := loadConfigV2(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘2’. %v", err)
	}
	if cv2.Version != "2" {
		return nil
	}

	srvConfig := &configV3{}
	srvConfig.Version = "3"
	srvConfig.Addr = ":9000"
	srvConfig.Credential = credential{
		AccessKey: cv2.Credentials.AccessKey,
		SecretKey: cv2.Credentials.SecretKey,
	}
	srvConfig.Region = cv2.Credentials.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature V4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = consoleLogger{
		Enable: true,
		Level:  "fatal",
	}
	flogger := fileLogger{}
	flogger.Level = "error"
	if cv2.FileLogger.Filename != "" {
		flogger.Enable = true
		flogger.Filename = cv2.FileLogger.Filename
	}
	srvConfig.Logger.File = flogger

	slogger := syslogLoggerV3{}
	slogger.Level = "debug"
	if cv2.SyslogLogger.Addr != "" {
		slogger.Enable = true
		slogger.Addr = cv2.SyslogLogger.Addr
	}
	srvConfig.Logger.Syslog = slogger

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize config. %v", err)
	}

	// Save configV3
	if err = qc.Save(configFile); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv2.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘2’ to ‘3’ completed successfully.")
	return nil
}

// Version '3' to '4' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV3ToV4(configFile string) error {
	cv3, err := loadConfigV3(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘3’. %v", err)
	}
	if cv3.Version != "3" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV4{}
	srvConfig.Version = "4"
	srvConfig.Credential = cv3.Credential
	srvConfig.Region = cv3.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv3.Logger.Console
	srvConfig.Logger.File = cv3.Logger.File
	srvConfig.Logger.Syslog = cv3.Logger.Syslog

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v", err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv3.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘3’ to ‘4’ completed successfully.")
	return nil
}

// Version '4' to '5' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV4ToV5(configFile string) error {
	cv4, err := loadConfigV4(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘4’. %v", err)
	}
	if cv4.Version != "4" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV5{}
	srvConfig.Version = "5"
	srvConfig.Credential = cv4.Credential
	srvConfig.Region = cv4.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv4.Logger.Console
	srvConfig.Logger.File = cv4.Logger.File
	srvConfig.Logger.Syslog = cv4.Logger.Syslog
	srvConfig.Logger.AMQP.Enable = false
	srvConfig.Logger.ElasticSearch.Enable = false
	srvConfig.Logger.Redis.Enable = false

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v", err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv4.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘4’ to ‘5’ completed successfully.")
	return nil
}

// Version '5' to '6' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV5ToV6(configFile string) error {
	cv5, err := loadConfigV5(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘5’. %v", err)
	}
	if cv5.Version != "5" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV6{}
	srvConfig.Version = "6"
	srvConfig.Credential = cv5.Credential
	srvConfig.Region = cv5.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv5.Logger.Console
	srvConfig.Logger.File = cv5.Logger.File
	srvConfig.Logger.Syslog = cv5.Logger.Syslog

	srvConfig.Notify.AMQP = map[string]amqpNotify{
		"1": {
			Enable:      cv5.Logger.AMQP.Enable,
			URL:         cv5.Logger.AMQP.URL,
			Exchange:    cv5.Logger.AMQP.Exchange,
			RoutingKey:  cv5.Logger.AMQP.RoutingKey,
			Mandatory:   cv5.Logger.AMQP.Mandatory,
			Immediate:   cv5.Logger.AMQP.Immediate,
			Durable:     cv5.Logger.AMQP.Durable,
			Internal:    cv5.Logger.AMQP.Internal,
			NoWait:      cv5.Logger.AMQP.NoWait,
			AutoDeleted: cv5.Logger.AMQP.AutoDeleted,
		},
	}
	srvConfig.Notify.ElasticSearch = map[string]elasticSearchNotify{
		"1": {
			Enable: cv5.Logger.ElasticSearch.Enable,
			URL:    cv5.Logger.ElasticSearch.URL,
			Index:  cv5.Logger.ElasticSearch.Index,
		},
	}
	srvConfig.Notify.Redis = map[string]redisNotify{
		"1": {
			Enable:   cv5.Logger.Redis.Enable,
			Addr:     cv5.Logger.Redis.Addr,
			Password: cv5.Logger.Redis.Password,
			Key:      cv5.Logger.Redis.Key,
		},
	}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v", err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv5.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘5’ to ‘6’ completed successfully.")
	return nil
}

// Version '6' to '7' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV6ToV7(configFile string) error {
	cv6, err := loadConfigV6(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘6’. %v", err)
	}
	if cv6.Version != "6" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &serverConfigV7{}
	srvConfig.Version = "7"
	srvConfig.Credential = cv6.Credential
	srvConfig.Region = cv6.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv6.Logger.Console
	srvConfig.Logger.File = cv6.Logger.File
	srvConfig.Logger.Syslog = cv6.Logger.Syslog
	srvConfig.Notify.AMQP = make(map[string]amqpNotify)
	srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
	srvConfig.Notify.Redis = make(map[string]redisNotify)
	if len(cv6.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv6.Notify.AMQP
	}
	if len(cv6.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv6.Notify.ElasticSearch
	}
	if len(cv6.Notify.Redis) == 0 {
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv6.Notify.Redis
	}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v", err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv6.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘6’ to ‘7’ completed successfully.")
	return nil
}

// Version '7' to '8' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV7ToV8(configFile string) error {
	cv7, err := loadConfigV7(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘7’. %v", err)
	}
	if cv7.Version != "7" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &serverConfigV8{}
	srvConfig.Version = "8"
	srvConfig.Credential = cv7.Credential
	srvConfig.Region = cv7.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv7.Logger.Console
	srvConfig.Logger.File = cv7.Logger.File
	srvConfig.Logger.Syslog = cv7.Logger.Syslog
	srvConfig.Notify.AMQP = make(map[string]amqpNotify)
	srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
	srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
	srvConfig.Notify.Redis = make(map[string]redisNotify)
	srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
	if len(cv7.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv7.Notify.AMQP
	}
	if len(cv7.Notify.NATS) == 0 {
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv7.Notify.NATS
	}
	if len(cv7.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv7.Notify.ElasticSearch
	}
	if len(cv7.Notify.Redis) == 0 {
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv7.Notify.Redis
	}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v", err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv7.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘7’ to ‘8’ completed successfully.")
	return nil
}

// Version '8' to '9' migration. Adds postgresql notifier
// configuration, but it's otherwise the same as V8.
func migrateV8ToV9(configFile string) error {
	cv8, err := loadConfigV8(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘8’. %v", err)
	}
	if cv8.Version != "8" {
		return nil
	}

	// Copy over fields from V8 into V9 config struct
	srvConfig := &serverConfigV9{}
	srvConfig.Version = "9"
	srvConfig.Credential = cv8.Credential
	srvConfig.Region = cv8.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv8.Logger.Console
	srvConfig.Logger.Console.Level = "error"
	srvConfig.Logger.File = cv8.Logger.File
	srvConfig.Logger.Syslog = cv8.Logger.Syslog

	// check and set notifiers config
	if len(cv8.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv8.Notify.AMQP
	}
	if len(cv8.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv8.Notify.NATS
	}
	if len(cv8.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv8.Notify.ElasticSearch
	}
	if len(cv8.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv8.Notify.Redis
	}
	if len(cv8.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv8.Notify.PostgreSQL
	}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v",
			err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv8.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘8’ to ‘9’ completed successfully.")
	return nil
}

// Version '9' to '10' migration. Remove syslog config
// but it's otherwise the same as V9.
func migrateV9ToV10(configFile string) error {
	cv9, err := loadConfigV9(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘9’. %v", err)
	}
	if cv9.Version != "9" {
		return nil
	}

	// Copy over fields from V9 into V10 config struct
	srvConfig := &serverConfigV10{}
	srvConfig.Version = "10"
	srvConfig.Credential = cv9.Credential
	srvConfig.Region = cv9.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv9.Logger.Console
	srvConfig.Logger.File = cv9.Logger.File

	// check and set notifiers config
	if len(cv9.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv9.Notify.AMQP
	}
	if len(cv9.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv9.Notify.NATS
	}
	if len(cv9.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv9.Notify.ElasticSearch
	}
	if len(cv9.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv9.Notify.Redis
	}
	if len(cv9.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv9.Notify.PostgreSQL
	}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v",
			err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv9.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘9’ to ‘10’ completed successfully.")
	return nil
}

// Version '10' to '11' migration. Add support for Kafka
// notifications.
func migrateV10ToV11(configFile string) error {
	cv10, err := loadConfigV10(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘10’. %v", err)
	}
	if cv10.Version != "10" {
		return nil
	}

	// Copy over fields from V10 into V11 config struct
	srvConfig := &serverConfigV11{}
	srvConfig.Version = "11"
	srvConfig.Credential = cv10.Credential
	srvConfig.Region = cv10.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv10.Logger.Console
	srvConfig.Logger.File = cv10.Logger.File

	// check and set notifiers config
	if len(cv10.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv10.Notify.AMQP
	}
	if len(cv10.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv10.Notify.NATS
	}
	if len(cv10.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv10.Notify.ElasticSearch
	}
	if len(cv10.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv10.Notify.Redis
	}
	if len(cv10.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv10.Notify.PostgreSQL
	}
	// V10 will not have a Kafka config. So we initialize one here.
	srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
	srvConfig.Notify.Kafka["1"] = kafkaNotify{}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v",
			err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv10.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘10’ to ‘11’ completed successfully.")
	return nil
}

// Version '11' to '12' migration. Add support for NATS streaming
// notifications.
func migrateV11ToV12(configFile string) error {
	cv11, err := loadConfigV11(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘11’. %v", err)
	}
	if cv11.Version != "11" {
		return nil
	}

	// Copy over fields from V11 into V12 config struct
	srvConfig := &serverConfigV12{}
	srvConfig.Version = "12"
	srvConfig.Credential = cv11.Credential
	srvConfig.Region = cv11.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv11.Logger.Console
	srvConfig.Logger.File = cv11.Logger.File

	// check and set notifiers config
	if len(cv11.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv11.Notify.AMQP
	}
	if len(cv11.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv11.Notify.ElasticSearch
	}
	if len(cv11.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv11.Notify.Redis
	}
	if len(cv11.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv11.Notify.PostgreSQL
	}
	if len(cv11.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv11.Notify.Kafka
	}

	// V12 will have an updated config of nats. So we create a new one or we
	// update the old one if found.
	if len(cv11.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		for k, v := range cv11.Notify.NATS {
			n := natsNotify{}
			n.Enable = v.Enable
			n.Address = v.Address
			n.Subject = v.Subject
			n.Username = v.Username
			n.Password = v.Password
			n.Token = v.Token
			n.Secure = v.Secure
			n.PingInterval = v.PingInterval
			srvConfig.Notify.NATS[k] = n
		}
	}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v",
			err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv11.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘11’ to ‘12’ completed successfully.")
	return nil
}

// Version '12' to '13' migration. Add support for custom webhook endpoint.
func migrateV12ToV13(configFile string) error {
	cv12, err := loadConfigV12(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘12’. %v", err)
	}
	if cv12.Version != "12" {
		return nil
	}

	// Copy over fields from V12 into V13 config struct
	srvConfig := &serverConfigV13{
		Logger: &logger{},
		Notify: &notifier{},
	}
	srvConfig.Version = "13"
	srvConfig.Credential = cv12.Credential
	srvConfig.Region = cv12.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv12.Logger.Console
	srvConfig.Logger.File = cv12.Logger.File

	// check and set notifiers config
	if len(cv12.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv12.Notify.AMQP
	}
	if len(cv12.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv12.Notify.ElasticSearch
	}
	if len(cv12.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv12.Notify.Redis
	}
	if len(cv12.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv12.Notify.PostgreSQL
	}
	if len(cv12.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv12.Notify.Kafka
	}
	if len(cv12.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv12.Notify.NATS
	}

	// V12 will not have a webhook config. So we initialize one here.
	srvConfig.Notify.Webhook = make(map[string]webhookNotify)
	srvConfig.Notify.Webhook["1"] = webhookNotify{}

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v",
			err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv12.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘12’ to ‘13’ completed successfully.")
	return nil
}

// Version '13' to '14' migration. Add support for custom webhook endpoint.
func migrateV13ToV14(configFile string) error {
	cv13, err := loadConfigV13(configFile)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘13’. %v", err)
	}
	if cv13.Version != "13" {
		return nil
	}

	// Copy over fields from V13 into V14 config struct
	srvConfig := &serverConfigV14{
		Logger: &logger{},
		Notify: &notifier{},
	}
	srvConfig.Version = "14"
	srvConfig.Credential = cv13.Credential
	srvConfig.Region = cv13.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv13.Logger.Console
	srvConfig.Logger.File = cv13.Logger.File

	// check and set notifiers config
	if len(cv13.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv13.Notify.AMQP
	}
	if len(cv13.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv13.Notify.ElasticSearch
	}
	if len(cv13.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv13.Notify.Redis
	}
	if len(cv13.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv13.Notify.PostgreSQL
	}
	if len(cv13.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv13.Notify.Kafka
	}
	if len(cv13.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv13.Notify.NATS
	}
	if len(cv13.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv13.Notify.Webhook
	}

	// Set the new browser parameter to true by default
	srvConfig.Browser = "on"

	qc, err := quick.New(srvConfig)
	if err != nil {
		return fmt.Errorf("Unable to initialize the quick config. %v",
			err)
	}

	err = qc.Save(configFile)
	if err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv13.Version, srvConfig.Version, err)
	}

	console.Println("Migration from version ‘13’ to ‘14’ completed successfully.")
	return nil
}
