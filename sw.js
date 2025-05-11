// Service Worker for image caching
const APP_CACHE_NAME = 'filmy-movie-browser-image-cache-v1';
const APP_ASSETS = [
  '/',
  'index.html',
  'js/filmy.js',
  'css/filmy.css',
  'manifest.json',
  'img/filmy192.webp',
  'img/filmy384.webp',
  'img/filmy512.webp',
  'img/filmy1024.webp',
  'img/filmy1024m.webp',
  'img/filmy1024.svg'
];
const CACHE_DURATION = 7 * 24 * 60 * 60 * 1000; // 7 days
const MAX_CACHE_SIZE = 250; // Maximum number of images to cache
const FAILED_REQUEST_EXPIRY = 24 * 60 * 60 * 1000; // 24 hours
const MAX_RETRY_ATTEMPTS = 3; // Maximum number of retry attempts

// Cache image URLs that match these patterns
const IMAGE_PATTERNS = [
  /\.jpg$/i,
  /\.jpeg$/i,
  /\.png$/i,
  /\.webp$/i,
  /\.gif$/i,
  /image\.tmdb\.org/i,
  /m\.media-amazon\.com/i
];

// In-memory cache for failed requests to prevent repeated fetching
const failedRequests = new Map();
// In-memory cache for retry counts
const retryAttempts = new Map();

self.addEventListener('install', event => {
  self.skipWaiting();
  console.log('Service worker installed');
  event.waitUntil(
    caches.open(APP_CACHE_NAME).then(cache => {
      return cache.addAll(APP_ASSETS);
    })
  );
});

self.addEventListener('activate', event => {
  console.log('Service worker activated');
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.filter(name => name !== APP_CACHE_NAME).map(name => {
          console.log(`Deleting old cache: ${name}`);
          return caches.delete(name);
        })
      );
    }).then(() => {
      console.log('Service worker claimed clients');
      return self.clients.claim();
    })
  );
});

self.addEventListener('fetch', event => {

  if (event.request.mode === 'navigate' && !navigator.onLine) {
    event.respondWith(
      caches.match('/offline.html')
        .then(response => {
          return response || new Response('Offline page not found', {
            status: 404,
            headers: { 'Content-Type': 'text/html' }
          });
        })
    );
    return;
  }

  const url = new URL(event.request.url);
  const urlString = url.href;

  // --- YOUTUBE THUMBNAIL NETWORK-FIRST HANDLING ### refactor, maybe with YT API Key ---
  if (
    (event.request.method === 'GET') &&
    (urlString.includes('img.youtube.com') || urlString.includes('i.ytimg.com'))
  ) {
    return;
  }

  // Only cache image requests
  const isImage = IMAGE_PATTERNS.some(pattern => pattern.test(urlString));
  const isAmazonOrTmdb =
    urlString.includes('m.media-amazon.com') ||
    urlString.includes('image.tmdb.org');

  if (event.request.method === 'GET' && isImage) {
    if (isAmazonOrTmdb) {
      // Always network-first for Amazon/TMDB images, only cache 200 OK
      event.respondWith(
        fetch(event.request.clone())
          .then(response => {
            if (response.ok) {
              // Clone the response and cache it properly
              const responseToCache = response.clone();
              caches.open(APP_CACHE_NAME)
                .then(cache => cache.put(event.request, responseToCache))
                .catch(err => console.error('Cache put error:', err));

              return response;
            }
            // If not ok, try cache as fallback
            return caches.open(APP_CACHE_NAME)
              .then(cache => {
                return cache.match(event.request, { ignoreSearch: true })
                  .then(cachedResponse => cachedResponse || response);
              });
          })
          .catch(() => {
            // If network fails, try cache as fallback
            return caches.open(APP_CACHE_NAME)
              .then(cache => {
                return cache.match(event.request, { ignoreSearch: true })
                  .then(cachedResponse => {
                    return cachedResponse ||
                      new Response('Image not available', {
                        status: 404,
                        headers: { 'Content-Type': 'text/plain' }
                      });
                  });
              });
          })
      );
      return;
    }

    // --- Existing cache-first logic for other images below ---

    // Check if this URL has recently failed
    const failedTime = failedRequests.get(urlString);
    if (failedTime && (Date.now() - failedTime < FAILED_REQUEST_EXPIRY)) {
      console.log(`Skipping previously failed request: ${urlString}`);
      event.respondWith(
        new Response('Image previously failed to load', {
          status: 404,
          headers: { 'Content-Type': 'text/plain' }
        })
      );
      return;
    }

    // Check retry attempts
    const attempts = retryAttempts.get(urlString) || 0;
    if (attempts >= MAX_RETRY_ATTEMPTS) {
      console.log(`Maximum retry attempts reached for: ${urlString}`);
      event.respondWith(
        new Response('Max retry attempts reached', {
          status: 429, // Too Many Requests
          headers: { 'Content-Type': 'text/plain' }
        })
      );
      return;
    }

    event.respondWith(
      caches.open(APP_CACHE_NAME).then(cache => {
        return cache.match(event.request, { ignoreSearch: true }).then(response => {
          // Check if we have a cached response
          if (response) {
            // Check if the cached response is still fresh
            const cachedDate = new Date(response.headers.get('date'));
            const now = new Date();

            if ((now - cachedDate) < CACHE_DURATION) {
              console.log(`Cache hit for: ${urlString}`);
              return response;
            }
            console.log(`Cache expired for: ${urlString}`);
          } else {
            console.log(`Cache miss for: ${urlString}`);
          }

          // If no cache or cache is stale, fetch from network
          // Implement exponential backoff for retries
          const backoffTime = Math.pow(2, attempts) * 1000; // Exponential backoff NYI ### refactor later

          // Update retry count
          retryAttempts.set(urlString, attempts + 1);

          console.log(`Fetching from network: ${urlString} (Attempt ${attempts + 1}/${MAX_RETRY_ATTEMPTS})`);

          return fetch(event.request)
            .then(networkResponse => {
              // Check if the response is successful
              if (networkResponse.ok) {
                console.log(`Successful fetch for: ${urlString}`);

                // Reset retry count on success
                retryAttempts.delete(urlString);
                // Remove from failed requests if it was there
                failedRequests.delete(urlString);

                // Clone the response since we need to use it twice
                const responseToCache = networkResponse.clone();

                // Cache the fresh response
                cache.put(event.request, responseToCache);

                // Monitor cache size after adding new item
                monitorCacheSize(cache);
              } else {
                console.warn(`Failed fetch (${networkResponse.status}) for: ${urlString}`);

                // Record this as a failed request if it's a 404
                if (networkResponse.status === 404) {
                  failedRequests.set(urlString, Date.now());
                  console.log(`Caching failed request for: ${urlString}`);

                  // Clean up old failed requests
                  cleanupFailedRequests();
                }
              }

              return networkResponse;
            })
            .catch(error => {
              console.error(`Fetch error for ${urlString}:`, error);

              // If it's a network error (offline), don't count as a failed request
              if (error.name !== 'TypeError') {
                failedRequests.set(urlString, Date.now());
              }

              // If offline or error, try to return cached version
              return response || new Response('Image not available', {
                status: 404,
                headers: { 'Content-Type': 'text/plain' }
              });
            });
        });
      }).catch(error => {
        console.error(`Cache error for ${urlString}:`, error);
        return new Response('Cache error', {
          status: 500,
          headers: { 'Content-Type': 'text/plain' }
        });
      })
    );
  }
});


// Helper function to monitor and limit cache size
async function monitorCacheSize(cache) {
  try {
    const requests = await cache.keys();

    if (requests.length > MAX_CACHE_SIZE) {
      console.log(`Cache size (${requests.length}) exceeds limit (${MAX_CACHE_SIZE}), cleaning up oldest items`);

      // Get all requests with their dates
      const requestsWithDates = await Promise.all(
        requests.map(async request => {
          const response = await cache.match(request);
          return {
            request,
            date: new Date(response.headers.get('date') || 0)
          };
        })
      );

      // Sort by date (oldest first) and remove excess
      requestsWithDates.sort((a, b) => a.date - b.date);
      const toRemove = requestsWithDates.slice(0, requests.length - MAX_CACHE_SIZE);

      await Promise.all(
        toRemove.map(item => cache.delete(item.request))
      );

      console.log(`Service worker removed ${toRemove.length} old cached images`);
    }
  } catch (error) {
    console.error('Error monitoring cache size:', error);
  }
}

// Helper function to clean up old failed requests
function cleanupFailedRequests() {
  const now = Date.now();
  for (const [url, time] of failedRequests.entries()) {
    if (now - time > FAILED_REQUEST_EXPIRY) {
      failedRequests.delete(url);
    }
  }
}

// Helper function to clean up old retry attempts
function cleanupRetryAttempts() {
  // Reset retry attempts periodically (every hour)
  const RETRY_RESET_INTERVAL = 60 * 60 * 1000; // 1 hour
  setInterval(() => {
    console.log('Resetting retry attempts');
    retryAttempts.clear();
  }, RETRY_RESET_INTERVAL);
}

// Start the retry cleanup timer
cleanupRetryAttempts();

// Handle various cache management messages
self.addEventListener('message', event => {
  if (!event.data || !event.data.action) return;

  console.log(`Received message: ${event.data.action}`);

  switch (event.data.action) {
    case 'cleanupCache':
      // Clean up based on age
      event.waitUntil(
        caches.open(APP_CACHE_NAME).then(cache => {
          return cache.keys().then(requests => {
            const now = new Date();

            return Promise.all(
              requests.map(request => {
                return cache.match(request).then(response => {
                  if (!response) return;

                  const cachedDate = new Date(response.headers.get('date'));
                  if ((now - cachedDate) > CACHE_DURATION) {
                    console.log(`Removing expired cache for: ${request.url}`);
                    return cache.delete(request);
                  }
                });
              })
            );
          });
        })
      );
      break;

    case 'monitorCacheSize':
      // Clean up based on total count
      event.waitUntil(
        caches.open(APP_CACHE_NAME).then(cache => monitorCacheSize(cache))
      );
      break;

    case 'clearFailedRequests':
      // Clear the failed requests cache
      failedRequests.clear();
      retryAttempts.clear();
      console.log('Cleared failed requests and retry attempts caches');
      break;

    case 'getCacheStats':
      // Return cache statistics to the client
      event.waitUntil(
        caches.open(APP_CACHE_NAME).then(async cache => {
          const requests = await cache.keys();
          const stats = {
            cacheSize: requests.length,
            failedRequestsSize: failedRequests.size,
            retryAttemptsSize: retryAttempts.size
          };

          // Send stats back to the client
          const client = await self.clients.get(event.source.id);
          if (client) {
            client.postMessage({
              action: 'cacheStats',
              stats
            });
          }
        })
      );
      break;

    default:
      console.log(`Unknown service worker action: ${event.data.action}`);
  }
});
