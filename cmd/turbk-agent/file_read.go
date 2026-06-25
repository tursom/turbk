package main

import (
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
				resultCh <- readAgentParallelFile(job, window)
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

func readAgentParallelFile(job agentParallelFileJob, window *agentFileReadByteWindow) agentParallelFileResult {
	file, err := agentOpen(job.filePath)
	if err != nil {
		return agentParallelFileResult{job: job, err: fmt.Errorf("open file %q: %w", job.filePath, err)}
	}
	chunker := repository.NewChunker(agentChunkAvgSize)
	var chunks [][]byte
	var acquired int64
	readErr := chunker.Split(file, func(chunk []byte) error {
		data := append([]byte(nil), chunk...)
		size := int64(len(data))
		window.acquire(size)
		acquired += size
		chunks = append(chunks, data)
		return nil
	})
	closeErr := file.Close()
	if readErr != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: fmt.Errorf("chunk file %q: %w", job.filePath, readErr)}
	}
	if closeErr != nil {
		window.release(acquired)
		return agentParallelFileResult{job: job, err: fmt.Errorf("close file %q: %w", job.filePath, closeErr)}
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
