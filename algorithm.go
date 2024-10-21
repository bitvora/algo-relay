package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var (
	weightInteractionsWithAuthor float64
	weightCommentsGlobal         float64
	weightReactionsGlobal        float64
	weightZapsGlobal             float64
	weightRecency                float64
	viralThreshold               float64
	viralPostDampening           float64
	decayRate                    float64
)

type CachedFeed struct {
	Feed      []FeedPost
	Timestamp time.Time
}

var userFeedCache sync.Map

const feedCacheDuration = 5 * time.Minute

var (
	pendingRequests      = make(map[string]chan struct{})
	pendingRequestsMutex sync.Mutex
)

func GetUserFeed(ctx context.Context, userID string, limit int) ([]nostr.Event, error) {
	now := time.Now()

	// Check if the feed is already cached
	if cached, ok := getCachedUserFeed(userID); ok && now.Sub(cached.Timestamp) < feedCacheDuration {
		log.Println("Returning cached feed for user:", userID)
		return createFeedResult(cached.Feed, limit), nil
	}

	// Check if feed generation is already pending
	pendingRequestsMutex.Lock()
	if pending, exists := pendingRequests[userID]; exists {
		log.Println("Waiting for existing feed generation for user:", userID)
		pendingRequestsMutex.Unlock()
		<-pending // Wait until the channel is closed
		if cached, ok := getCachedUserFeed(userID); ok && now.Sub(cached.Timestamp) < feedCacheDuration {
			return createFeedResult(cached.Feed, limit), nil
		}
		return nil, fmt.Errorf("feed generation failed after waiting for cache")
	}

	// Otherwise, set up a new pending request for this user
	pending := make(chan struct{})
	pendingRequests[userID] = pending
	pendingRequestsMutex.Unlock()

	defer func() {
		pendingRequestsMutex.Lock()
		close(pending) // Signal that feed generation is complete
		delete(pendingRequests, userID)
		pendingRequestsMutex.Unlock()
	}()

	// Generate the feed
	log.Println("No cache or pending request found, generating feed for user:", userID)
	authorFeed, err := repository.GetUserFeedByAuthors(ctx, userID, limit/2)
	if err != nil {
		return nil, err
	}

	// Get viral posts from cache
	viralPostCacheMutex.Lock()
	viralFeed := viralPostCache.Posts
	viralPostCacheMutex.Unlock()

	combinedFeed := append(authorFeed, viralFeed...)

	// Track the number of posts per author
	authorPostCount := make(map[string]int)
	filteredFeed := make([]FeedPost, 0, len(combinedFeed))
	for _, feedPost := range combinedFeed {
		authorID := feedPost.Event.PubKey
		if authorID == userID {
			continue
		}
		if authorPostCount[authorID] < 1 {
			filteredFeed = append(filteredFeed, feedPost)
			authorPostCount[authorID]++
		}
	}

	// Sort by score in descending order
	sort.Slice(filteredFeed, func(i, j int) bool {
		return filteredFeed[i].Score > filteredFeed[j].Score
	})

	// Cache the filtered feed
	userFeedCache.Store(userID, CachedFeed{
		Feed:      filteredFeed,
		Timestamp: now,
	})

	return createFeedResult(filteredFeed, limit), nil
}

func getCachedUserFeed(userID string) (CachedFeed, bool) {
	if cached, ok := userFeedCache.Load(userID); ok {
		return cached.(CachedFeed), true
	}
	return CachedFeed{}, false
}

func createFeedResult(filteredFeed []FeedPost, limit int) []nostr.Event {
	result := make([]nostr.Event, 0, len(filteredFeed))
	authorAppearance := make(map[string]int)

	for _, feedPost := range filteredFeed {
		authorID := feedPost.Event.PubKey

		if authorAppearance[authorID] > 0 && authorAppearance[authorID]%20 == 0 {
			continue // Skip if the author has appeared in the last 20 posts
		}

		result = append(result, feedPost.Event)
		authorAppearance[authorID]++
		if len(result) >= limit {
			break
		}
	}

	return result
}

func (r *NostrRepository) GetUserFeedByAuthors(ctx context.Context, userID string, limit int) ([]FeedPost, error) {
	authorInteractions, err := r.fetchTopInteractedAuthors(userID)
	if err != nil {
		return nil, err
	}

	posts, err := r.fetchPostsFromAuthors(authorInteractions)
	if err != nil {
		return nil, err
	}

	feedPosts := make([]FeedPost, len(posts))
	for p, post := range posts {
		interactionCount := getInteractionCountForAuthor(post.Event.PubKey, authorInteractions)
		score := r.calculateAuthorPostScore(post, interactionCount)
		feedPosts[p] = FeedPost{Event: post.Event, Score: score}
	}

	sort.Slice(feedPosts, func(i, j int) bool {
		return feedPosts[i].Score > feedPosts[j].Score
	})

	return feedPosts, nil
}

func getInteractionCountForAuthor(authorID string, interactions []AuthorInteraction) int {
	for _, interaction := range interactions {
		if interaction.AuthorID == authorID {
			return interaction.InteractionCount
		}
	}
	return 0
}

func (r *NostrRepository) calculateAuthorPostScore(event EventWithMeta, interactionCount int) float64 {
	recencyFactor := calculateRecencyFactor(event.CreatedAt)

	score := float64(event.GlobalCommentsCount)*weightCommentsGlobal +
		float64(event.GlobalReactionsCount)*weightReactionsGlobal +
		float64(event.GlobalZapsCount)*weightZapsGlobal +
		recencyFactor*weightRecency +
		float64(interactionCount)*weightInteractionsWithAuthor

	return score
}

func calculateRecencyFactor(createdAt time.Time) float64 {
	hoursSinceCreation := time.Since(createdAt).Hours()

	// Use a scaling factor to normalize recency
	scalingFactor := 100.0
	recencyFactor := math.Exp(-decayRate*hoursSinceCreation) * scalingFactor

	// Cap and floor the recency factor to avoid extreme values
	if recencyFactor > 1.0 {
		recencyFactor = 1.0
	} else if recencyFactor < 0.001 {
		recencyFactor = 0.001
	}

	return recencyFactor
}

func getWeightFloat64(envKey string) float64 {
	weight := os.Getenv(envKey)

	// Log the environment key and value for debugging purposes
	log.Printf("Fetching environment variable for %s: %s", envKey, weight)

	// Trim any extra spaces from the environment variable
	weight = strings.TrimSpace(weight)

	if weight == "" {
		log.Printf("Environment variable %s not set, defaulting to 1", envKey)
		return 1
	}

	// Parse the float value
	w, err := strconv.ParseFloat(weight, 64)
	if err != nil {
		// Log the error and return default
		log.Printf("Error parsing float for %s: %v, defaulting to 1", envKey, err)
		return 1
	}

	return w
}
