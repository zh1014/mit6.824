package raft

import (
	"fmt"
	"mit6.824/labrpc"
	"strconv"
	"strings"
	"testing"
)

func Test_adjustNextIndex(t *testing.T) {
	l := &Log{
		commitIndex:     485,
		lastApplied:     485,
		lastIncluded:    402,
		lastIncludeTerm: 4,
		leaderState: &LeaderState{
			nextIndex: make([]int, 5),
		},
	}
	l.entries = parseStringEntries("403{term=4,cmd=731}, 404{term=4,cmd=6390}, 405{term=4,cmd=7264}, 406{term=4,cmd=5937}, 407{term=4,cmd=554}, 408{term=4,cmd=4952}, 409{term=4,cmd=8678}, 410{term=4,cmd=6322}, 411{term=4,cmd=9306}, 412{term=4,cmd=9993}, 413{term=4,cmd=7936}, 414{term=4,cmd=7190}, 415{term=4,cmd=4115}, 416{term=4,cmd=8764}, 417{term=4,cmd=9977}, 418{term=4,cmd=3024}, 419{term=4,cmd=5444}, 420{term=4,cmd=7095}, 421{term=4,cmd=6860}, 422{term=4,cmd=5472}, 423{term=4,cmd=5695}, 424{term=4,cmd=7967}, 425{term=4,cmd=4376}, 426{term=4,cmd=7788}, 427{term=4,cmd=9932}, 428{term=4,cmd=3790}, 429{term=4,cmd=4465}, 430{term=4,cmd=4847}, 431{term=4,cmd=2226}, 432{term=4,cmd=6554}, 433{term=4,cmd=7079}, 434{term=4,cmd=2445}, 435{term=4,cmd=5467}, 436{term=4,cmd=1802}, 437{term=4,cmd=5275}, 438{term=4,cmd=6630}, 439{term=4,cmd=9808}, 440{term=4,cmd=3117}, 441{term=4,cmd=4815}, 442{term=4,cmd=2475}, 443{term=4,cmd=8465}, 444{term=4,cmd=5627}, 445{term=4,cmd=6683}, 446{term=4,cmd=2830}, 447{term=4,cmd=3185}, 448{term=4,cmd=5542}, 449{term=4,cmd=4007}, 450{term=4,cmd=7271}, 451{term=4,cmd=4669}, 452{term=4,cmd=2515}, 453{term=4,cmd=7971}, 454{term=4,cmd=3855}, 455{term=4,cmd=7766}, 456{term=10,cmd=6738}, 457{term=10,cmd=3328}, 458{term=10,cmd=7468}, 459{term=10,cmd=233}, 460{term=10,cmd=920}, 461{term=10,cmd=1908}, 462{term=10,cmd=1826}, 463{term=10,cmd=4985}, 464{term=10,cmd=9912}, 465{term=10,cmd=7396}, 466{term=10,cmd=7458}, 467{term=10,cmd=5819}, 468{term=10,cmd=4509}, 469{term=10,cmd=9942}, 470{term=10,cmd=127}, 471{term=10,cmd=2580}, 472{term=10,cmd=6037}, 473{term=10,cmd=1092}, 474{term=10,cmd=2655}, 475{term=10,cmd=4077}, 476{term=10,cmd=2557}, 477{term=10,cmd=5985}, 478{term=10,cmd=9268}, 479{term=10,cmd=1828}, 480{term=10,cmd=9342}, 481{term=10,cmd=3713}, 482{term=10,cmd=4316}, 483{term=10,cmd=6285}, 484{term=10,cmd=3225}, 485{term=12,cmd=3365}, 486{term=12,cmd=3365}")
	ok := l.adjustNextIndex(1, 4, 0)
	if ok {
		t.Errorf("should not ok")
	}
	fmt.Println("result:", l.leaderState.nextIndex[1])
}

func parseStringEntries(str string) []*labrpc.LogEntry {
	var entries []*labrpc.LogEntry
	strSli := strings.Split(str, ", ")
	for _, subStr := range strSli {
		i0 := strings.IndexByte(subStr, '=') + 1
		i1 := strings.IndexByte(subStr, ',')
		i2 := strings.LastIndexByte(subStr, '=') + 1
		i3 := strings.IndexByte(subStr, '}')
		term, err := strconv.Atoi(subStr[i0:i1])
		checkErr(err)
		cmd, err := strconv.Atoi(subStr[i2:i3])
		checkErr(err)
		entries = append(entries, &labrpc.LogEntry{
			Term: term,
			Cmd:  cmd,
		})
	}
	return entries
}
