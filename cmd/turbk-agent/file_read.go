package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tursom/turbk/internal/repository"
)

type agentParallelFileJob struct {
	root        string
	catalogPath string
	filePath    string
	entry       repository.FileEntry
	record      catalogFileRecord
}

type agentParallelFileResult struct {
	job    agentParallelFileJob
	chunks [][]byte
	bytes  int64
	err    error
}

type agentFileReadByteWindow struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int64
	used  int64
	max   int64
}

func newAgentFileReadByteWindow(limit int64) *agentFileReadByteWindow {
	if limit <= 0 {
		limit = defaultAgentFileReadPipelineBytes
	}
	w := &agentFileReadByteWindow{limit: limit}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *agentFileReadByteWindow) acquire(n int64) {
	if w == nil || n <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.used > 0 && w.used+n > w.limit {
		w.cond.Wait()
	}
	w.used += n
	if w.used > w.max {
		w.max = w.used
	}
}

func (w *agentFileReadByteWindow) release(n int64) {
	if w == nil || n <= 0 {
		return
	}
	w.mu.Lock()
	w.used -= n
	if w.used < 0 {
		w.used = 0
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

func (w *agentFileReadByteWindow) maxBytes() int64 {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max
}

func runAgentParallelFileReaders(jobs []agentParallelFileJob, workers int, pipelineBytes int64) (<-chan agentParallelFileResult, *agentFileReadByteWindow) {
	if workers <= 0 {
		workers = 2
	}
	if workers > len(jobs) && len(jobs) > 0 {
		workers = len(jobs)
	}
	window := newAgentFileReadByteWindow(pipelineBytes)
	jobCh := make(chan agentParallelFileJob)
	resultCh := make(chan agentParallelFileResult)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				resultCh <- readAgentParallelFileWithRetry(job, window)
			}
		}()
	}
	go func() {
		for _, job := range jobs {
			jobCh <- job
		}
		close(jobCh)
		wg.Wait()
		close(resultCh)
	}()
	return resultCh, window
}

func readAgentParallelFileWithRetry(job agentParallelFileJob, window *agentFileReadByteWindow) agentParallelFileResult {
	for attempt := 0; attempt <= defaultAgentChangedFileReadMaxRetries; attempt++ {
		result := readAgentParallelFileOnce(job, window)
		if result.err == nil {
			return result
		}
		if !errors.Is(result.err, errAgentPathChanged) || attempt >= defaultAgentChangedFileReadMaxRetries {
			return result
		}
		refreshedEntry, refreshedRecord, err := refreshedRegularFileReadState(job.root, job.catalogPath, job.filePath, job.entry)
		if err != nil {
			return agentParallelFileResult{job: job, err: err}
		}
		job.entry = refreshedEntry
		job.record = refreshedRecord
	}
	return agentParallelFileResult{job: job, err: changedPathError{path: job.filePath}}
}

func readAgentParallelFileOnce(job agentParallelFileJob, window *agentFileReadByteWindow) agentParallelFileResult {
	file, err := agentOpen(job.filePath)
	if err != nil {
		return agentParallelFileResult{job: job, err: fmt.Errorf("open file %q: %w", job.filePath, err)}
	}
	chunker := repository.NewChunker(agentChunkAvgSize)
	reader := newRegularFileSnapshotReader(job.root, job.catalogPath, job.filePath, file, job.record)
	var chunks [][]byte
	var acquired int64
	readErr := chunker.Split(reader, func(chunk []byte) error {
		data := append([]byte(nil), chunk...)
		size := int64(len(data))
		window.acquire(size)
		acquired += size
		chunks = append(chunks, data)
		return nil
	})
	stat, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: fmt.Errorf("chunk file %q: %w", job.filePath, readErr)}
	}
	if statErr != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: fmt.Errorf("stat file %q after read: %w", job.filePath, statErr)}
	}
	if closeErr != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: fmt.Errorf("close file %q: %w", job.filePath, closeErr)}
	}
	if err := updateRegularFileReadMetadata(job.root, job.catalogPath, job.filePath, reader.BytesRead(), stat, &job.entry, &job.record); err != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: err}
	}
	return agentParallelFileResult{job: job, chunks: chunks, bytes: acquired}
}

func releaseParallelFileChunks(window *agentFileReadByteWindow, chunks [][]byte) {
	for _, chunk := range chunks {
		window.release(int64(len(chunk)))
	}
}

func fileReadWindowMaxBytes(window *agentFileReadByteWindow) int64 {
	if window == nil {
		return 0
	}
	return window.maxBytes()
}
