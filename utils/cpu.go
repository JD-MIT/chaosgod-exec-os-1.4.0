package utils

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var cpuNum int

func init() {

	quotaFile, err := os.Open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	if err != nil {
		fmt.Println(err)
	}
	periodFile, err := os.Open("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err != nil {
		fmt.Println(err)
	}
	defer quotaFile.Close()
	defer periodFile.Close()

	rd := bufio.NewReader(quotaFile)
	quotaStr, _, err := rd.ReadLine()

	if err != nil {
		fmt.Println(err)
	}
	cpuQuota, err := strconv.Atoi(string(quotaStr))

	period := bufio.NewReader(periodFile)
	periodStr, _, err := period.ReadLine()

	if err != nil {
		fmt.Println(err)
	}
	cpuPeriod, err := strconv.Atoi(string(periodStr))

	if err != nil {
		fmt.Println(err)
	}

	if cpuQuota != -1 {

		cpuNum = cpuQuota / cpuPeriod
	} else {
		cpuNum = runtime.NumCPU()
	}
}

func CpuPercent(interval time.Duration) ([]float64, error) {
	return PercentWithContext(context.Background(), interval)
}

func PercentWithContext(ctx context.Context, interval time.Duration) ([]float64, error) {

	cpuTimes := make([]float64, 0)
	first := getCpuAcct()

	if interval <= 0 {
		interval = time.Second
	}
	time.Sleep(interval)
	latest := getCpuAcct()
	//防止溢出
	//if latest < first {
	//	first = getCpuAcct()
	//	time.Sleep(interval)
	//	last = getCpuAcct()
	//}
	//容器的CPU使用率用总使用率需要除以容器核心数 * 100%，才能和物理机取到的CPU使用率的计算方式一致
	//容器需要考虑超卖的情况
	cpuTimes = append(cpuTimes, (latest-first)/float64(cpuNum))

	return cpuTimes, nil
}

func getCpuAcctValue(s string) float64 {
	ss := strings.Trim(strings.Split(s, " ")[1], "\n")
	cpuTime, _ := strconv.ParseFloat(ss, 64)
	return cpuTime
}

func getCpuAcct() float64 {
	//容器的CPU使用率用总使用率取值文件
	file, err := os.Open("/sys/fs/cgroup/cpu/cpuacct.stat")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	rd := bufio.NewReader(file)
	s := make([]string, 0)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
		s = append(s, line)
		if err != nil || io.EOF == err {
			break
		}
	}

	userCpuTime := getCpuAcctValue(s[0])
	sysCpuTime := getCpuAcctValue(s[1])

	return userCpuTime + sysCpuTime
}

func CPUNum() int {
	return cpuNum
}
