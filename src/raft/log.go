package raft

type LogEntry struct {
	Term    int         // term of log entry
	Command interface{} // Command
}

//
// 在加入snapshot之后，需要discard掉snapshot之前的logs
// 这导致如果用slice来存储log的话，log index和它在slice上的下标就不对应了
// 所以声明了Log这个struct，用于处理需要discard logs的情况
//
type Log struct {
	LastIncludedIndex int        // 最后一个被Snapshot的entry的index
	LastIncludedTerm  int        // 最后一个被Snapshot的entry的index
	Log               []LogEntry // 实际存放log的slice, index of log[i] = i + LastIncludedIndex
}

// 获取log[index]
func (l *Log) Get(index int) LogEntry {
	if index <= l.LastIncludedIndex || index > l.LastIncludedIndex+len(l.Log) {
		return LogEntry{}
	}
	return l.Log[index-l.LastIncludedIndex-1]
}

//
// 获取对应index的term
// 如果index == LastIncludedIndex，返回LastIncludedTerm
// 如果index > LastIncludedIndex，返回Log中记录的Term
//
func (l *Log) GetTerm(index int) int {
	if index == l.LastIncludedIndex {
		return l.LastIncludedTerm
	}
	if index < l.LastIncludedIndex || index > l.LastIncludedIndex+len(l.Log) {
		return -1
	}
	return l.Get(index).Term
}

// 左闭右开，[indexL, indexR)
func (l *Log) GetSlice(indexL, indexR int) []LogEntry {
	if indexL < l.LastIncludedIndex || indexL > indexR {
		return []LogEntry{}
	}

	if indexR < 0 || indexR > l.LastIncludedIndex+len(l.Log) {
		indexR = l.LastIncludedIndex + len(l.Log) + 1
	}
	indexL = indexL - l.LastIncludedIndex - 1
	indexR = indexR - l.LastIncludedIndex - 1

	return l.Log[indexL:indexR]
}

// 丢弃<=index的log entries，并且更新lastIncludedIndex和lastIncludedTerm
func (l *Log) Discard(index int) bool {
	if index < l.LastIncludedIndex || index > l.LastIncludedIndex+len(l.Log) {
		return false
	}

	i := index - l.LastIncludedIndex - 1
	lastIncludedIndex := index
	lastIncludedTerm := l.GetTerm(index)
	l.Log = l.Log[i+1:]
	l.LastIncludedIndex = lastIncludedIndex
	l.LastIncludedTerm = lastIncludedTerm
	return true
}

//
// 丢弃所有的logs，并更新lastIncludedIndex和lastIncludedTerm
//
func (l *Log) DiscardAll(lastIncludedIndex, lastIncludedTerm int) {
	l.Log = []LogEntry{}
	l.LastIncludedIndex = lastIncludedIndex
	l.LastIncludedTerm = lastIncludedTerm
}

// 从index之后append（保留index，后面的全部discard）
func (l *Log) Append(index int, entries []LogEntry) bool {
	// fmt.Printf("append: index:%v, lastIncludedIndex:%v, entries:%v, log:%v\n", index, l.LastIncludedIndex, entries, l.Log)

	if index < l.LastIncludedIndex || index > l.GetLastEntryIndex() {
		return false
	}

	i := index - l.LastIncludedIndex - 1
	l.Log = append(l.Log[:i+1], entries...)
	// fmt.Printf("append finish: index:%v, lastIncludedIndex:%v, entries:%v, log:%v\n", index, l.LastIncludedIndex, entries, l.Log)
	return true
}

func (l *Log) AppendSingle(entry LogEntry) {
	// fmt.Printf("appendsingle: lastIncludedIndex:%v, entries:%v, log:%v\n", l.LastIncludedIndex, entry, l.Log)
	l.Log = append(l.Log, entry)
	// fmt.Printf("appendsingle finish: lastIncludedIndex:%v, entries:%v, log:%v\n", l.LastIncludedIndex, entry, l.Log)
}

//
// 返回index对应的term的前一个term最后一个index
// 比如term: 2 2 3 3，返回第二个2的index
//
func (l *Log) GetPrevTermIndex(index, term int) int {
	if index < l.LastIncludedIndex {
		// 符合要求的index已经被discard了
		return -1
	}

	index = index - l.LastIncludedIndex - 1
	L, R, i := -1, index+1, 0
	for L+1 < R {
		k := (L + R) / 2
		if l.Log[k].Term == term {
			R, i = k, k
		} else {
			L = k
		}
	}
	i--

	if i < 0 {
		if l.LastIncludedTerm == term {
			// 符合要求的index已经被discard了
			return -1
		}
		return l.LastIncludedIndex
	}
	return i + l.LastIncludedIndex + 1
}

// log中返回<=index的entries里，第一个term小于等于给定term的index
func (l *Log) GetLessAndEqualTermIndex(index, term int) int {
	if index < l.LastIncludedIndex {
		// 符合要求的index已经被discard了
		return -1
	}

	index = index - l.LastIncludedIndex - 1
	L, R, i := -1, index+1, -1
	for L+1 < R {
		k := (L + R) / 2
		if l.Log[k].Term <= term {
			L, i = k, k
		} else {
			R = k
		}
	}

	return i
}

// 获取log中最后一个entry的index
func (l *Log) GetLastEntryIndex() int {
	return l.LastIncludedIndex + len(l.Log)
}
