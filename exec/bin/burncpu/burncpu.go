/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package main

import (
	"context"
	"flag"
	"fmt"
	cpuPercentUtils "github.com/chaosblade-io/chaosblade-exec-os/utils"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chaosblade-io/chaosblade-spec-go/channel"
	"github.com/chaosblade-io/chaosblade-spec-go/spec"
	"github.com/chaosblade-io/chaosblade-spec-go/util"
	"github.com/shirou/gopsutil/process"

	"github.com/chaosblade-io/chaosblade-exec-os/exec"
	"github.com/chaosblade-io/chaosblade-exec-os/exec/bin"

	_ "go.uber.org/automaxprocs/maxprocs"
)

var (
	burnCpuStart, burnCpuStop, burnCpuNohup, absolute bool
	cpuCount, cpuPercent, climbTime                   int
	slopePercent                                      float64
	cpuList                                           string
	cpuProcessor                                      string
)

func main() {
	flag.BoolVar(&burnCpuStart, "start", false, "start burn cpu")
	flag.BoolVar(&burnCpuStop, "stop", false, "stop burn cpu")
	flag.StringVar(&cpuList, "cpu-list", "", "CPUs in which to allow burning (1,3)")
	flag.BoolVar(&burnCpuNohup, "nohup", false, "nohup to run burn cpu")
	flag.IntVar(&climbTime, "climb-time", 0, "durations(s) to climb")
	flag.IntVar(&cpuCount, "cpu-count", cpuPercentUtils.CPUNum(), "number of cpus")
	flag.IntVar(&cpuPercent, "cpu-percent", 100, "percent of burn-cpu")
	flag.StringVar(&cpuProcessor, "cpu-processor", "0", "only used for identifying process of cpu burn")
	flag.BoolVar(&absolute, "absolute", false, "run burn cpu absolutely")

	bin.ParseFlagAndInitLog()

	if cpuCount <= 0 || cpuCount > cpuPercentUtils.CPUNum() {
		cpuCount = cpuPercentUtils.CPUNum()
	}

	if burnCpuStart {
		startBurnCpu()
	} else if burnCpuStop {
		if success, errs := stopBurnCpuFunc(); !success {
			bin.PrintErrAndExit(errs)
		}
	} else if burnCpuNohup {
		burnCpu()
	} else {
		bin.PrintErrAndExit("less --start or --stop flag")
	}
}

func burnCpu() {

	runtime.GOMAXPROCS(cpuCount)

	var totalCpuPercent []float64
	var curProcess *process.Process
	var curCpuPercent float64
	var err error

	totalCpuPercent, err = cpuPercentUtils.CpuPercent(time.Second)
	if err != nil {
		bin.PrintErrAndExit(err.Error())
	}

	curProcess, err = process.NewProcess(int32(os.Getpid()))
	if err != nil {
		bin.PrintErrAndExit(err.Error())
	}

	curCpuPercent, err = curProcess.CPUPercent()
	if err != nil {
		bin.PrintErrAndExit(err.Error())
	}

	otherCpuPercent := (100.0 - (totalCpuPercent[0] - curCpuPercent)) / 100.0
	go func() {
		t := time.NewTicker(3 * time.Second)
		for {
			select {
			// timer 3s
			case <-t.C:
				totalCpuPercent, err = cpuPercentUtils.CpuPercent(time.Second)
				if err != nil {
					bin.PrintErrAndExit(err.Error())
				}

				curCpuPercent, err = curProcess.CPUPercent()
				if err != nil {
					bin.PrintErrAndExit(err.Error())
				}
				otherCpuPercent = (100.0 - (totalCpuPercent[0] - curCpuPercent)) / 100.0
			}
		}
	}()

	if climbTime == 0 {
		slopePercent = float64(cpuPercent)
	} else {
		// TODO 此处应该处理 absolute 的处理情形
		var ticker *time.Ticker = time.NewTicker(1 * time.Second)
		slopePercent = totalCpuPercent[0]
		var startPercent = float64(cpuPercent) - slopePercent
		go func() {
			for range ticker.C {
				if slopePercent < float64(cpuPercent) {
					slopePercent += startPercent / float64(climbTime)
				} else if slopePercent > float64(cpuPercent) {
					slopePercent -= startPercent / float64(climbTime)
				}
			}
		}()
	}

	for i := 0; i < cpuCount; i++ {
		go func() {
			busy := int64(0)
			idle := int64(0)
			all := int64(10000000)
			dx := 0.0
			ds := time.Duration(0)
			for i := 0; ; i = (i + 1) % 1000 {
				startTime := time.Now().UnixNano()
				if i == 0 {
					if absolute {
						dx = (slopePercent - (curCpuPercent / float64(cpuCount))) / otherCpuPercent
					} else {
						dx = (slopePercent - totalCpuPercent[0]) / otherCpuPercent
					}
					busy = busy + int64(dx*100000)
					if busy < 0 {
						busy = 0
					}
					idle = all - busy
					if idle < 0 {
						idle = 0
					}
					ds, _ = time.ParseDuration(strconv.FormatInt(idle, 10) + "ns")
				}
				for time.Now().UnixNano()-startTime < busy {
				}
				time.Sleep(ds)
				runtime.Gosched()
			}
		}()
	}
	select {}
}

var burnCpuBin = exec.BurnCpuBin

var cl = channel.NewLocalChannel()

var stopBurnCpuFunc = stopBurnCpu

var runBurnCpuFunc = runBurnCpu

var bindBurnCpuFunc = bindBurnCpuByTaskset

var checkBurnCpuFunc = checkBurnCpu

// startBurnCpu by invoke burnCpuBin with --nohup flag
func startBurnCpu() {
	ctx := context.Background()
	if cpuList != "" {
		cpuCount = 1
		cores := strings.Split(cpuList, ",")
		for _, core := range cores {
			pid := runBurnCpuFunc(ctx, cpuCount, cpuPercent, true, core, climbTime, absolute)
			bindBurnCpuFunc(ctx, core, pid)
		}
	} else {
		runBurnCpuFunc(ctx, cpuCount, cpuPercent, false, "", climbTime, absolute)
	}
	checkBurnCpuFunc(ctx)
}

// runBurnCpu
func runBurnCpu(ctx context.Context, cpuCount int, cpuPercent int, pidNeeded bool, processor string, climbTime int, absolute bool) int {
	args := fmt.Sprintf(`%s --nohup --cpu-count %d --cpu-percent %d --climb-time %d`,
		path.Join(util.GetProgramPath(), burnCpuBin), cpuCount, cpuPercent, climbTime)
	if pidNeeded {
		args = fmt.Sprintf("%s --cpu-processor %s", args, processor)
	}
	if absolute {
		args = fmt.Sprintf("%s --absolute", args)
	}
	args = fmt.Sprintf(`%s > /dev/null 2>&1 &`, args)
	response := cl.Run(ctx, "nohup", args)
	if !response.Success {
		stopBurnCpuFunc()
		bin.PrintErrAndExit(response.Err)
	}
	if pidNeeded {
		// parse pid
		newCtx := context.WithValue(context.Background(), channel.ProcessKey, fmt.Sprintf("cpu-processor %s", processor))
		pids, err := cl.GetPidsByProcessName(burnCpuBin, newCtx)
		if err != nil {
			stopBurnCpuFunc()
			bin.PrintErrAndExit(fmt.Sprintf("bind cpu core failed, cannot get the burning program pid, %v", err))
		}
		if len(pids) > 0 {
			// return the first one
			pid, err := strconv.Atoi(pids[0])
			if err != nil {
				stopBurnCpuFunc()
				bin.PrintErrAndExit(fmt.Sprintf("bind cpu core failed, get pid failed, pids: %v, err: %v", pids, err))
			}
			return pid
		}
	}
	return -1
}

// bindBurnCpu by taskset command
func bindBurnCpuByTaskset(ctx context.Context, core string, pid int) {
	if !cl.IsCommandAvailable("taskset") {
		bin.PrintErrAndExit(spec.CommandTasksetNotFound.Msg)
	}
	response := cl.Run(ctx, "taskset", fmt.Sprintf("-a -cp %s %d", core, pid))
	if !response.Success {
		stopBurnCpuFunc()
		bin.PrintErrAndExit(response.Err)
	}
}

// checkBurnCpu
func checkBurnCpu(ctx context.Context) {
	time.Sleep(time.Second)
	// query process
	ctx = context.WithValue(ctx, channel.ProcessKey, "nohup")
	pids, _ := cl.GetPidsByProcessName(burnCpuBin, ctx)
	if pids == nil || len(pids) == 0 {
		bin.PrintErrAndExit(fmt.Sprintf("%s pid not found", burnCpuBin))
	}
}

// stopBurnCpu
func stopBurnCpu() (success bool, errs string) {
	// add grep nohup
	ctx := context.WithValue(context.Background(), channel.ProcessKey, "nohup")
	pids, _ := cl.GetPidsByProcessName(burnCpuBin, ctx)
	if pids == nil || len(pids) == 0 {
		return true, errs
	}
	response := cl.Run(ctx, "kill", fmt.Sprintf(`-9 %s`, strings.Join(pids, " ")))
	if !response.Success {
		return false, response.Err
	}
	return true, errs
}
