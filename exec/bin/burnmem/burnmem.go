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
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/chaosblade-io/chaosblade-spec-go/channel"
	"github.com/chaosblade-io/chaosblade-spec-go/spec"
	"github.com/chaosblade-io/chaosblade-spec-go/util"
	"github.com/containerd/cgroups"
	v1 "github.com/containerd/cgroups/stats/v1"
	"github.com/shirou/gopsutil/mem"

	"github.com/sirupsen/logrus"

	"github.com/chaosblade-io/chaosblade-exec-os/exec"
	"github.com/chaosblade-io/chaosblade-exec-os/exec/bin"
)

const PageCounterMax uint64 = 9223372036854770000

const (
	processOOMScoreAdj = "/proc/%s/oom_score_adj"
	oomMinScore        = "-1000"
)

// 128K
type Block [32 * 1024]int32

var (
	burnMemStart, burnMemStop, burnMemNohup, includeBufferCache, avoidBeingKilled, rateFlag, isHost bool
	memPercent, memReserve, memRate                                                                 int
	burnMemMode                                                                                     string
)

func init() {
	//	var logfile *os.File
	//	var err error
	//	logfile, err = os.OpenFile("./chaos_burnmem.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	//	if err != nil {
	//		fmt.Println("OPen log failed err:", err)
	//		return
	//	}
	//	log.SetOutput(logfile)
	//	log.SetPrefix("[ chaos-server ]")
	//	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.Ldate)
	isHost = !isCout()
}

func main() {
	flag.BoolVar(&burnMemStart, "start", false, "start burn memory")
	flag.BoolVar(&burnMemStop, "stop", false, "stop burn memory")
	flag.BoolVar(&burnMemNohup, "nohup", false, "nohup to run burn memory")
	flag.BoolVar(&includeBufferCache, "include-buffer-cache", false, "ram model mem-percent is exclude buffer/cache")
	flag.BoolVar(&avoidBeingKilled, "avoid-being-killed", false, "prevent mem-burn process from being killed by oom-killer")
	flag.IntVar(&memPercent, "mem-percent", 0, "percent of burn memory")
	flag.IntVar(&memReserve, "reserve", 0, "reserve to burn memory, unit is M")
	flag.IntVar(&memRate, "rate", 0, "burn memory rate, unit is M/S, only support for ram mode")
	flag.StringVar(&burnMemMode, "mode", "cache", "burn memory mode, cache or ram")
	bin.ParseFlagAndInitLog()

	if memRate <= 0 {
		rateFlag = false
	} else {
		rateFlag = true
	}
	if burnMemStart {
		startBurnMem()
	} else if burnMemStop {
		if success, errs := stopBurnMem(); !success {
			bin.PrintErrAndExit(errs)
		}
	} else if burnMemNohup {
		if burnMemMode == "cache" {
			burnMemWithCache()
		} else if burnMemMode == "ram" {
			burnMemWithRam()
		}
	} else {
		bin.PrintAndExitWithErrPrefix("less --start or --stop flag")
	}

}

var dirName = "burnmem_tmpfs"

var fileName = "file"

var fileCount = 1

func burnMemWithRam() {
	tick := time.Tick(time.Second)
	var cache = make(map[int][]Block, 1)
	var count = 1
	cache[count] = make([]Block, 0)
	state := sync.Mutex{}
	for range tick {
		state.Lock()
		_, expectMem, err := calculateMemSize(memPercent, memReserve)
		if !rateFlag {

			if expectMem > 2048 {
				memRate = 1024
			} else {
				memRate = 100
			}
		}
		if err != nil {
			stopBurnMemFunc()
			bin.PrintErrAndExit(err.Error())
		}
		fillMem := expectMem
		if expectMem > 0 {
			if expectMem > int64(memRate) {
				fillMem = int64(memRate)
			} else {
				fillMem = expectMem / 10
				if fillMem == 0 {
					continue
				}
			}
			fillSize := int(8 * fillMem)
			buf := cache[count]
			if cap(buf)-len(buf) < fillSize &&
				int(math.Floor(float64(cap(buf))*1.25)) >= int(8*expectMem) {
				count += 1
				cache[count] = make([]Block, 0)
				buf = cache[count]
			}
			//log.Printf("count: %d, len(buf): %d, cap(buf): %d, expect mem: %d, fill size: %d",
			//	count, len(buf), cap(buf), expectMem, fillSize)
			//log.Printf("%s",fillSize)
			cache[count] = append(buf, make([]Block, fillSize)...)
			state.Unlock()
		}
	}
}

func burnMemWithCache() {
	filePath := path.Join(path.Join(util.GetProgramPath(), dirName), fileName)
	tick := time.Tick(time.Second)
	for range tick {
		_, expectMem, err := calculateMemSize(memPercent, memReserve)
		if err != nil {
			stopBurnMemFunc()
			bin.PrintErrAndExit(err.Error())
		}
		fillMem := expectMem
		if expectMem > 0 {
			if expectMem > int64(memRate) {
				fillMem = int64(memRate)
			}
			nFilePath := fmt.Sprintf("%s%d", filePath, fileCount)
			response := cl.Run(context.Background(), "dd", fmt.Sprintf("if=/dev/zero of=%s bs=1M count=%d", nFilePath, fillMem))
			if !response.Success {
				stopBurnMemFunc()
				bin.PrintErrAndExit(response.Error())
			}
			fileCount++
		}
	}
}

var burnMemBin = exec.BurnMemBin

var cl = channel.NewLocalChannel()

var stopBurnMemFunc = stopBurnMem

var runBurnMemFunc = runBurnMem

func startBurnMem() {
	ctx := context.Background()
	if burnMemMode == "cache" {
		if !cl.IsCommandAvailable("mount") {
			bin.PrintErrAndExit(spec.CommandMountNotFound.Msg)
		}

		flPath := path.Join(util.GetProgramPath(), dirName)
		if _, err := os.Stat(flPath); err != nil {
			err = os.Mkdir(flPath, os.ModePerm)
			if err != nil {
				bin.PrintErrAndExit(err.Error())
			}
		}
		response := cl.Run(ctx, "mount", fmt.Sprintf("-t tmpfs tmpfs %s -o size=", flPath)+"100%")
		if !response.Success {
			bin.PrintErrAndExit(response.Error())
		}
	}
	runBurnMemFunc(ctx, memPercent, memReserve, memRate, burnMemMode, includeBufferCache)
}

func runBurnMem(ctx context.Context, memPercent, memReserve, memRate int, burnMemMode string, includeBufferCache bool) {
	args := fmt.Sprintf(`%s --nohup --mem-percent %d --reserve %d --rate %d --mode %s --include-buffer-cache=%t`,
		path.Join(util.GetProgramPath(), burnMemBin), memPercent, memReserve, memRate, burnMemMode, includeBufferCache)
	args = fmt.Sprintf(`%s > /dev/null 2>&1 &`, args)
	response := cl.Run(ctx, "nohup", args)
	if !response.Success {
		stopBurnMemFunc()
		bin.PrintErrAndExit(response.Err)
	}
	// check pid
	newCtx := context.WithValue(context.Background(), channel.ProcessKey, "--nohup")
	pids, err := cl.GetPidsByProcessName(burnMemBin, newCtx)
	if err != nil {
		stopBurnMemFunc()
		bin.PrintErrAndExit(fmt.Sprintf("run burn memory by %s mode failed, cannot get the burning program pid, %v",
			burnMemMode, err))
	}
	if len(pids) == 0 {
		stopBurnMemFunc()
		bin.PrintErrAndExit(fmt.Sprintf("run burn memory by %s mode failed, cannot find the burning program pid",
			burnMemMode))
	}
	// adjust process oom_score_adj to avoid being killed
	if avoidBeingKilled {
		for _, pid := range pids {
			scoreAdjFile := fmt.Sprintf(processOOMScoreAdj, pid)
			if _, err := os.Stat(scoreAdjFile); os.IsNotExist(err) {
				continue
			}

			if err := ioutil.WriteFile(scoreAdjFile, []byte(oomMinScore), 0644); err != nil {
				stopBurnMemFunc()
				bin.PrintErrAndExit(fmt.Sprintf("run burn memory by %s mode failed, cannot edit the process oom_score_adj",
					burnMemMode))
			}
		}
	}
}

func stopBurnMem() (success bool, errs string) {
	ctx := context.WithValue(context.Background(), channel.ProcessKey, "nohup")
	ctx = context.WithValue(ctx, channel.ExcludeProcessKey, "stop")
	pids, _ := cl.GetPidsByProcessName(burnMemBin, ctx)
	var response *spec.Response
	if pids != nil && len(pids) != 0 {
		response = cl.Run(ctx, "kill", fmt.Sprintf(`-9 %s`, strings.Join(pids, " ")))
		if !response.Success {
			return false, response.Err
		}
	}
	if burnMemMode == "cache" {
		dirPath := path.Join(util.GetProgramPath(), dirName)
		if _, err := os.Stat(dirPath); err == nil {
			if !cl.IsCommandAvailable("umount") {
				bin.PrintErrAndExit(spec.CommandUmountNotFound.Msg)
			}

			response = cl.Run(ctx, "umount", dirPath)
			if !response.Success {
				if !strings.Contains(response.Err, "not mounted") {
					bin.PrintErrAndExit(response.Error())
				}
			}
			err = os.RemoveAll(dirPath)
			if err != nil {
				bin.PrintErrAndExit(err.Error())
			}
		}
	}
	return true, errs
}

func calculateMemSize(percent, reserve int) (int64, int64, error) {
	total := int64(0)
	available := int64(0)
	memoryStat, err := getMemoryStatsByCGroup()
	if err != nil {
		logrus.Infof("get memory stats by cgroup failed, used proc memory, %v", err)
	}
	if memoryStat == nil || memoryStat.Usage.Limit >= PageCounterMax {
		//no limit
		virtualMemory, err := mem.VirtualMemory()
		if err != nil {
			return 0, 0, err
		}
		total = int64(virtualMemory.Total)
		available = int64(virtualMemory.Free)
		if burnMemMode == "ram" && !includeBufferCache {
			available = available + int64(virtualMemory.Buffers+virtualMemory.Cached)
		}
	} else {
		total = int64(memoryStat.Usage.Limit)
		available = total - int64(memoryStat.ActiveAnon+memoryStat.InactiveAnon)
		if burnMemMode == "ram" && !includeBufferCache {
			available = available + int64(memoryStat.Cache)
		}
	}
	reserved := int64(0)
	if percent != 0 {
		reserved = (total * int64(100-percent) / 100) / 1024 / 1024
	} else {
		reserved = int64(reserve)
	}

	if !isHost {
		activeAnon := int64(memoryStat.ActiveAnon)
		inactiveAnon := int64(memoryStat.InactiveAnon)
		used := int64(activeAnon + inactiveAnon)
		expectSize := (total*int64(percent)/100 - used) / 1024 / 1024
		//log.Printf("isCout")
		//log.Printf("total: %d, used: %d, percent: %d, expectSize: %d",
		//	total/1024/1024, used/1024/1024, percent, expectSize)
		//
		//log.Printf("available: %d, percent: %d, reserved: %d, expectSize: %d",
		//	available/1024/1024, percent, reserved, expectSize)

		return total / 1024 / 1024, expectSize, nil
	}

	expectSize := available/1024/1024 - reserved
	//log.Printf("total: %d, percent: %d, expectSize: %d",
	//	total/1024/1024, percent, expectSize)
	//log.Printf("available: %d, percent: %d, reserved: %d, expectSize: %d",
	//	available/1024/1024, percent, reserved, expectSize)

	return total / 1024 / 1024, expectSize, nil
}

func getMemoryStatsByCGroup() (*v1.MemoryStat, error) {
	cgroup, err := cgroups.Load(cgroups.V1, cgroups.StaticPath("/"))
	if err != nil {
		return nil, fmt.Errorf("load cgroup error, %v", err)
	}
	stats, err := cgroup.Stat(cgroups.IgnoreNotExist)
	if err != nil {
		return nil, fmt.Errorf("load cgroup stat error, %v", err)
	}
	return stats.Memory, nil
}

func isCout() bool {
	cgroupfile, err := os.Open("/proc/1/cgroup")
	if err != nil {
		fmt.Println(err)
	}
	defer cgroupfile.Close()

	rd := bufio.NewReader(cgroupfile)
	if err != nil {
		fmt.Println(err)
	}
	cgroup, _, err := rd.ReadLine()

	return strings.Contains(string(cgroup), "kubepods")
}
