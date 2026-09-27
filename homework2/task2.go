package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

const baseURL = "https://homeworksite.site"

type Movie struct {
	ID       int    `json:"id"`
	Title    string `json:"title"`
	Year     int    `json:"year"`
	Director string `json:"director"`
}

type Config struct {
	From    int
	To      int
	Workers int
	Timeout time.Duration
}

func parseFlags(args []string) (*Config, error) {
	fs := flag.NewFlagSet("movie-loader", flag.ContinueOnError)

	fromPtr := fs.Int("from", 0, "id первого фильма (обязательный)")
	toPtr := fs.Int("to", 0, "id последнего фильма (обязательный)")
	workersPtr := fs.Int("workers", 10, "количество воркеров в worker pool")
	timeoutPtr := fs.Duration("timeout", 5*time.Second, "таймаут одного HTTP-запроса")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })

	if !seen["from"] {
		return nil, errors.New("обязательный флаг --from не указан")
	}
	if !seen["to"] {
		return nil, errors.New("обязательный флаг --to не указан")
	}

	cfg := &Config{
		From:    *fromPtr,
		To:      *toPtr,
		Workers: *workersPtr,
		Timeout: *timeoutPtr,
	}

	if cfg.From > cfg.To {
		return nil, fmt.Errorf("--from (%d) не может быть больше --to (%d)", cfg.From, cfg.To)
	}
	if cfg.Workers <= 0 {
		return nil, fmt.Errorf("--workers должен быть положительным, получено %d", cfg.Workers)
	}
	if cfg.Timeout <= 0 {
		return nil, fmt.Errorf("--timeout должен быть положительным, получено %s", cfg.Timeout)
	}

	return cfg, nil
}

type fetchResult struct {
	ID    int
	Movie Movie
	Err   error
}

func fetchMovie(ctx context.Context, client *http.Client, timeout time.Duration, id int) fetchResult {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := fmt.Sprintf("%s/%d/info.0.json", baseURL, id)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return fetchResult{ID: id, Err: fmt.Errorf("создание запроса: %w", err)}
	}

	resp, err := client.Do(req)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return fetchResult{ID: id, Err: fmt.Errorf("таймаут запроса (%s)", timeout)}
		case errors.Is(err, context.Canceled):
			return fetchResult{ID: id, Err: errors.New("запрос отменён")}
		default:
			return fetchResult{ID: id, Err: fmt.Errorf("обрыв соединения: %w", err)}
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fetchResult{ID: id, Err: fmt.Errorf("сервер вернул статус %d", resp.StatusCode)}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fetchResult{ID: id, Err: fmt.Errorf("чтение тела ответа: %w", err)}
	}

	var m Movie
	if err := json.Unmarshal(body, &m); err != nil {
		return fetchResult{ID: id, Err: fmt.Errorf("битый JSON: %w", err)}
	}
	if m.ID == 0 {
		m.ID = id
	}

	return fetchResult{ID: id, Movie: m}
}

func run(ctx context.Context, cfg *Config, client *http.Client) []fetchResult {
	ids := make(chan int)
	resultsCh := make(chan fetchResult)

	var wg sync.WaitGroup
	wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go func() {
			defer wg.Done()
			for id := range ids {
				resultsCh <- fetchMovie(ctx, client, cfg.Timeout, id)
			}
		}()
	}

	go func() {
		defer close(ids)
		for id := cfg.From; id <= cfg.To; id++ {
			select {
			case ids <- id:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]fetchResult, 0, cfg.To-cfg.From+1)
	for r := range resultsCh {
		results = append(results, r)
	}
	return results
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		flag.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{}

	start := time.Now()
	results := run(ctx, cfg, client)
	elapsed := time.Since(start)

	sort.Slice(results, func(i, j int) bool { return results[i].ID < results[j].ID })

	var failed int
	for _, r := range results {
		if r.Err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "фильм %d: %v\n", r.ID, r.Err)
			continue
		}
		fmt.Printf("%d — %s — %d — %s\n", r.Movie.ID, r.Movie.Title, r.Movie.Year, r.Movie.Director)
	}

	total := cfg.To - cfg.From + 1
	fmt.Fprintf(os.Stderr, "\nготово за %s: %d/%d фильмов загружено успешно (workers=%d, timeout=%s)\n",
		elapsed.Round(time.Millisecond), total-failed, total, cfg.Workers, cfg.Timeout)

	if ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "прервано пользователем (Ctrl-C)")
		os.Exit(130)
	}
}
