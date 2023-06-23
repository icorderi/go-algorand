// Copyright (C) 2019-2023 Algorand, Inc.
// This file is part of go-algorand
//
// go-algorand is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// go-algorand is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with go-algorand.  If not, see <https://www.gnu.org/licenses/>.

package node

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/algorand/go-algorand/config"
)

func registerSignals(rootDir string, cfg *config.Local) {
	sig := make(chan os.Signal, 1)

	signal.Notify(sig, syscall.SIGUSR1)

	go func() {
		for {
			_ = <-sig

			buf := make([]byte, 1<<20)
			stacklen := runtime.Stack(buf, true)
			f, err := os.Create(rootDir + "/algod-stack.log")
			if err != nil {
				continue
			}
			// pprof.Lookup("goroutine").WriteTo(f, 1)
			f.Write(buf[:stacklen])
			f.Close()
		}
	}()
}
