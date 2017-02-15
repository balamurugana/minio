/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

	"github.com/pkg/profile"
)

// Global profiler to be used by service go-routine.
var globalProfiler interface {
	Stop()
}

func checkProfileMode(mode string) error {
	switch mode {
	case "":
		fallthrough
	case "cpu":
		fallthrough
	case "mem":
		fallthrough
	case "block":
		return nil
	}

	return fmt.Errorf("unknown profile mode `%s'", mode)
}

// Starts a profiler returns nil if profiler is not enabled, caller needs to handle this.
func startProfiler(profiler string) interface {
	Stop()
} {
	// Enable profiler if ``_MINIO_PROFILER`` is set. Supported options are [cpu, mem, block].
	switch profiler {
	case "cpu":
		return profile.Start(profile.CPUProfile, profile.NoShutdownHook)
	case "mem":
		return profile.Start(profile.MemProfile, profile.NoShutdownHook)
	case "block":
		return profile.Start(profile.BlockProfile, profile.NoShutdownHook)
	default:
		return nil
	}
}
