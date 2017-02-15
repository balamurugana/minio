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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// Test if config v1 is purged
func TestMigrateConfigPurgeV1(t *testing.T) {
	tmpDir, err := ioutil.TempFile("", "migrate-testdir")
	if err != nil {
		t.Fatalf("Unable to create temporary directory. %s", err)
	}
	defer removeAll(tmpDir)

	// Create a V1 config json file and store it
	configJSON := `{"version":"1", "accessKeyId":"abcde", "secretAccessKey":"abcdefgh"}`
	configFile := filepath.Join(tmpDir, "fsUsers.json")
	if err := ioutil.WriteFile(configFile, []byte(configJSON), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Fire a migrateConfig()
	if err := migrateConfig(configFile); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Check if config v1 is removed from filesystem
	if _, err := os.Stat(configFile); !os.IsNotExist(err) {
		t.Fatal("Config V1 file is not purged")
	}
}

// Test if all migrate code returns nil when config file does not
// exist
func TestMigrateConfigNonexistentConfig(t *testing.T) {
	nonexistentConfigFile = "nonexistent-config-file"

	if err := migrateV2ToV3(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v2 to v3 should succeed when no config file is found")
	}
	if err := migrateV3ToV4(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v3 to v4 should succeed when no config file is found")
	}
	if err := migrateV4ToV5(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v4 to v5 should succeed when no config file is found")
	}
	if err := migrateV5ToV6(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v5 to v6 should succeed when no config file is found")
	}
	if err := migrateV6ToV7(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v6 to v7 should succeed when no config file is found")
	}
	if err := migrateV7ToV8(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v7 to v8 should succeed when no config file is found")
	}
	if err := migrateV8ToV9(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v8 to v9 should succeed when no config file is found")
	}
	if err := migrateV9ToV10(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v9 to v10 should succeed when no config file is found")
	}
	if err := migrateV10ToV11(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v10 to v11 should succeed when no config file is found")
	}
	if err := migrateV11ToV12(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v11 to v12 should succeed when no config file is found")
	}
	if err := migrateV12ToV13(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v12 to v13 should succeed when no config file is found")
	}
	if err := migrateV13ToV14(nonexistentConfigFile); err != nil {
		t.Fatal("migrate v13 to v14 should succeed when no config file is found")
	}
}

// Test if all migrate code returns error with corrupted config files
func TestMigrateConfigCurruptedConfig(t *testing.T) {
	tmpConfigFile, err := ioutil.TempFile("", "migrate-test")
	if err != nil {
		t.Fatalf("Unable to create temporary file. %s", err)
	}
	defer os.Remove(tmpConfigFile.Name())

	// Create a corrupted config file
	if err := ioutil.WriteFile(configPath, []byte(`{"version":"2",`), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	// Fire a migrateConfig()
	if err := migrateConfig(tmpConfigFile); err == nil {
		t.Fatal("migration should fail with corrupted config file")
	}

	// Test different migrate versions and be sure they are returning an error
	if err := migrateV2ToV3(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV2ToV3() should fail with a corrupted json")
	}
	if err := migrateV3ToV4(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV3ToV4() should fail with a corrupted json")
	}
	if err := migrateV4ToV5(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV4ToV5() should fail with a corrupted json")
	}
	if err := migrateV5ToV6(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV5ToV6() should fail with a corrupted json")
	}
	if err := migrateV6ToV7(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV6ToV7() should fail with a corrupted json")
	}
	if err := migrateV7ToV8(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV7ToV8() should fail with a corrupted json")
	}
	if err := migrateV8ToV9(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV8ToV9() should fail with a corrupted json")
	}
	if err := migrateV9ToV10(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV9ToV10() should fail with a corrupted json")
	}
	if err := migrateV10ToV11(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV10ToV11() should fail with a corrupted json")
	}
	if err := migrateV11ToV12(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV11ToV12() should fail with a corrupted json")
	}
	if err := migrateV12ToV13(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV12ToV13() should fail with a corrupted json")
	}
	if err := migrateV13ToV14(tmpConfigFile); err == nil {
		t.Fatal("migrateConfigV13ToV14() should fail with a corrupted json")
	}
}

// Test if a config migration from v2 to v12 is successfully done
func TestMigrateConfigValidConfig(t *testing.T) {
	tmpConfigFile, err := ioutil.TempFile("", "migrate-test")
	if err != nil {
		t.Fatalf("Unable to create temporary file. %s", err)
	}
	defer os.Remove(tmpConfigFile.Name())

	accessKey := "accessfoo"
	secretKey := "secretfoo"

	// Create a V2 config json file and store it
	configJSON := fmt.Sprintf(`{"version":"2", "credentials": {"accessKeyId":"%s", "secretAccessKey":"%s", "region":"us-east-1"}, "mongoLogger":{"addr":"127.0.0.1:3543", "db":"foodb", "collection":"foo"}, "syslogLogger":{"network":"127.0.0.1:543", "addr":"addr"}, "fileLogger":{"filename":"log.out"}}`,
		accessKey, secretKey)
	if err := ioutil.WriteFile(tmpConfigFile, []byte(configJSON), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	// Fire a migrateConfig()
	if err := migrateConfig(tmpConfigFile); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// TODO: load tmpConfigFile and validate its configuration.
}
