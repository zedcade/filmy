/** 
 * filmy v1.0 Beta | (c) 2025 Alexander Ipfelkofer | MIT License
 * A Movie & Series Web App
 * Leveraging IndexedDB and LocalStorage
 * filmy provides a seamless experience for cataloguing and exploring your personal media collection
 * 
 * API keys from OMDB and TMDB are required for the script to operate
 * 
 * Fully developed through AI-assisted coding techniques
 * Built with pure JavaScript - No frameworks, No external libraries, No Headaches
 * 
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * This web application uses The Movie Database (TMDB) and the OMDB API but is not endorsed, certified, or otherwise approved by TMDB or OMDB.
 * This web application provides links to IMDb, Metacritic, and Rotten Tomatoes for reference purposes only. We are not affiliated with or endorsed by these services.
 * 
*/

/* GLOBAL VARS */
const app_name = "filmy";
const app_version = "1.0 Beta";
const app_desc = `<strong>A Movie & Series Web App<br/>
Leveraging IndexedDB and LocalStorage</strong>
<p>Built with pure JavaScript - no frameworks or external libraries.<br/>
Developed through AI-assisted coding techniques.</p>
<p><i>${app_name}</i> provides a seamless experience for cataloguing and exploring your personal media collection.</p>`;

const api_notice = `<div class="api-notice">
  <p>This application requires API keys from <a href="https://www.themoviedb.org/settings/api/" target="_blank">TMDB</a> and <a href="https://www.omdbapi.com/apikey.aspx" target="_blank">OMDb</a>.</p>
  <p><em>New users: Visit the Settings menu after initial page load for instructions on obtaining free API keys.</em></p>
</div>`;

const current_features = `<div class="features-section">

<h3 class="section-heading">Current Features</h3>
<h4 class="subsection-heading">Keyboard Shortcuts — Grid View</h4>
<ul class="feature-list">
  <li><span class="feature-name">[ N ]</span> Add a note for media card in focus</li>
  <li><span class="feature-name">[ D ]</span> Open Detail View for media card in focus</li>
  <li><span class="feature-name">[ T ]</span> Play trailer for media card in focus</li>
  <li><span class="feature-name">[ R ]</span> Refresh media card in focus</li>
</ul>
<h4 class="subsection-heading">Keyboard Shortcuts — Detail View</h4>
<ul class="feature-list">
  <li><span class="feature-name">[ SPACE ]</span> Backdrop View: Start Slideshow</li>
  <li><span class="feature-name">[ Left ]</span> Backdrop View: Previous image</li>
  <li><span class="feature-name">[ Right ]</span> Backdrop View: Next image</li>
  <li><span class="feature-name">[ X ]</span> Close Popup</li>
  <li><span class="feature-name">[ ESC ]</span> Close Popup</li>

</ul>
<h4 class="subsection-heading">Content Management</h4>
<ul class="feature-list">
  <li><span class="feature-name">Smart Media Addition:</span> Easily add movies and series through intuitive TMDb/OMDb search</li>
  <li><span class="feature-name">Custom Poster Selection:</span> Personalize your collection with handpicked posters and backdrops</li>
  <li><span class="feature-name">Watchlist Import:</span> Bring your existing IMDb movie lists into your collection</li>
</ul>

<h4 class="subsection-heading">User Interface</h4>
<ul class="feature-list">
  <li><span class="feature-name">Dynamic Poster Grid:</span> Browse your collection in a responsive layout with adjustable poster sizes (50-300px)</li>
  <li><span class="feature-name">Instant Information:</span> Access ratings, trailers, and key details via elegant hover tooltips</li>
  <li><span class="feature-name">Advanced Filtering:</span> Narrow results by genre, year, ratings (IMDb, TMDb, Metacritic, Rotten Tomatoes), and country</li>
  <li><span class="feature-name">Smart Search:</span> Find content across multiple categories (movies, series, people, keywords)</li>
</ul>

<h4 class="subsection-heading">Media Experience</h4>
<ul class="feature-list">
  <li><span class="feature-name">Immersive Backdrop Slideshows:</span> Explore high-quality production images in the detail view</li>
  <li><span class="feature-name">Integrated Trailer Playback:</span> Watch trailers without leaving the application</li>
  <li><span class="feature-name">Comprehensive Cast & Crew:</span> Discover the people behind your favorite content</li>
</ul>

<h4 class="subsection-heading">Connectivity & Utility</h4>
<ul class="feature-list">
  <li><span class="feature-name">External Service Integration:</span> One-click access to IMDb, TMDB, Metacritic, and Rotten Tomatoes</li>
  <li><span class="feature-name">JustWatch Integration:</span> Find where to stream or purchase content</li>
  <li><span class="feature-name">Database Management:</span> Back up and restore your collection with simple JSON exports</li>
  <li><span class="feature-name">Resource Efficiency:</span> Utilizes inline SVG symbols with zero external dependencies</li>
</ul>
</div>`;

const feature_roadmap = `<div class="roadmap-section">
<h3 class="section-heading">Feature Roadmap</h3>

<h4 class="subsection-heading">High Priority - Core Experience Enhancements</h4>
<h5 class="subsection-heading">Content Organization</h5>
<ul class="feature-list">
  <li>Stack movies by sets/collections</li>
  <li>Latest and Popular/Trending info</li>
  <li>Favorite posters and backdrops with regional filtering</li>
</ul>

<h5 class="subsection-heading">User Experience</h5>
<ul class="feature-list">
  <li>Improved touch and mobile support</li>
  <li>Watch Provider/JustWatch integration</li>
  <li>Collapsible detail view with card stacking</li>
  <li>Theme support (Light Mode, Ultra Noir)</li>
</ul>

<h4 class="subsection-heading">Medium Priority - Expanded Functionality</h4>
<h5 class="subsection-heading">Personal Curation</h5>
<ul class="feature-list">
  <li>Improved Notes Feature for movies, series, and episodes</li>
  <li>User profile improvements (stats, achievements)</li>
  <li>Add Import handling from various external sources</li>
</ul>

<h5 class="subsection-heading">Discovery Tools</h5>
<ul class="feature-list">
  <li>Content randomizer by genre, year, and other criteria</li>
  <li>Soundtrack Feature</li>
  <li>Galaxy View: visual discovery interface</li>
</ul>

<h4 class="subsection-heading">Future Enhancements</h4>
<h5 class="subsection-heading">Social Features</h5>
<ul class="feature-list">
  <li>Share movies, series, and episodes</li>
  <li>Enhanced Handling of Multiple User Profiles</li>
</ul>

<h5 class="subsection-heading">Advanced Features</h5>
<ul class="feature-list">
  <li>Multiple Choice Movie Quiz</li>
  <li>Advanced database management (DB store controls, purging options)</li>
  <li>Offline/online mode switching, pouchDB support</li>
</ul>
</div>`;

const about_content = `
${app_desc}
${api_notice}
${current_features}
<hr>
${feature_roadmap}
<hr>
`;

const app_disclaimer = `<p><strong>MIT License</strong><br/>

Copyright (c) 2025 Alexander Ipfelkofer</p>

<p>Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:</p>

<p>The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.</p>

<p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.</p>
`;

const app_attribution = `<p>This web application uses The Movie Database (TMDB) and the OMDB API but is not endorsed, certified, or otherwise approved by TMDB or OMDB.</p>
<p>This web application provides links to IMDb, Metacritic, and Rotten Tomatoes for reference purposes only. We are not affiliated with or endorsed by these services.</p>`;
const app_copyright = "(c) 2025 Alexander Ipfelkofer | MIT License";

/* DB related */
const DB_NAME = 'filmyDb';
const DB_VERSION = 1; // increase for schema update
let db;

/* cache related */
let userSettings = {}; 
let mediaSettingsCache = {};
let notifications = [];

const renderedMediaIDs = new Set();
let mediaCache = new Map(); 
let genreCache = [];
let activeGenres = []; 
let activeMediaType = null;
let previousMediaType = null;
let filteredRecords = [];
let listsMap = {}; 
let activeLists = [];
let allListsMeta = [];
let customListsMeta = [];

/* tooltip and UI related */
let closeButtonTimeout; // Timeout for hiding video close button
const videoStatusCache = new Map();

/* detail view & slideshow */
let slideshowInterval = null;
let fadeTimeout = null;
let currentBackdrops = [];
let currentBackdropIndex = 0;
let initialJWConfig = {};

/* filters */
let activeSearchQuery = null; // Track the currently active search query
let searchAbortController = null; // For canceling in-progress searches
let activeCountry = "All";
let activeStartYear = null;
let activeEndYear = null;
let defaultStartYear = null;
let defaultEndYear = null;
let yearRangeMin, yearRangeMax, startYearLabel, endYearLabel;
let yearSliderTrack, minYear, maxYear;
let yearSortAscending = true;
let activeMinImdb = null;
let activeMaxImdb = null;
let activeMinTmdb = null;
let activeMaxTmdb = null;
let activeMinMetascore = null;
let activeMaxMetascore = null;
let activeMinRT = null;
let activeMaxRT = null;
let ratingsInitialized = false;
let currentSortField = null; // 'imdb', 'tmdb', 'metascore', 'rt', or null (default = title)
let currentSortOrder = 'desc'; // 'desc', 'asc'

/* Global Variables for Grid Navigation */
const DEFAULT_CARD_SIZE = 300; // Default card size (px)
let pageSize = 24;              // Number of movies to load per batch
let observer = null;            // IntersectionObserver instance
let totalRecords = 0;           // Total records count (if needed)
let currentJumpLetter = null;   // Last jumped letter
let currentJumpIndex = null;    // Last jumped record index
let startIndex = 0;             // Current batch start index
let endIndex = 0;               // Current batch end index
let letterIndex = null;         // Letter index map for quick jumps
let isObserving = false; // Track observer state globally
let isJumping = false;
let jumpLock = false;         // Prevent lazy loading immediately

// Initialize isAtMaxSize based on the current grid size slider value
let isAtMaxSize = (() => {
  const gridSizeSlider = document.getElementById('gridSizeSlider');
  if (gridSizeSlider) {
    const size = parseInt(gridSizeSlider.value, 10);
    const sliderMax = parseInt(gridSizeSlider.max, 10);
    return size >= sliderMax;
  }
  return false;
})();

let lastBatchLoadTime = 0;
const BATCH_THROTTLE_INTERVAL = 300; // ms between batch loads

// PWA install
let deferredPrompt;

/** 
 * API related
*/
let isFetching = false; // Prevent multiple fetches at once
const omdbRateLimit = 1000; // Daily limit per OMDB key
const tmdbThrottleLimit = 50;
const tmdbThrottleInterval = 1000; // 1 second
const omdbApiBaseUrl = "https://www.omdbapi.com/";
const tmdbApiBaseUrl = "https://api.themoviedb.org/3/";
const tmdbImgBaseUrl = "https://image.tmdb.org/t/p/";
const tmdbBaseUrl = "https://www.themoviedb.org/"

/* Context map for TMDB endpoints to transform and store in indexedDB */
const endpointContextMap = {
  credits: "person",
  aggregate_credits: "person",
  people: "person",
  keywords: "keyword",
  release_dates: "movie",
};

/* Arrays to store series, people, and keywords data */       
const peopleArray = [];
const keywordsArray = [];

/* Unified media cache mapping with type filtering and credit stores */
const searchMapping = {
  Movies: { 
    items: () => Array.from(mediaCache.values()).filter(m => m.mediaType === 'movie'),
    fields: ["Title", "original_title"],
    creditStore: "movie_credits" 
  },
  Series: { 
    items: () => Array.from(mediaCache.values()).filter(m => m.mediaType === 'tv'),
    fields: ["Title", "original_title"], 
    creditStore: "series_credits" 
  },
  People: {
    items: () => peopleArray,
    fields: ["name"],
    lookupFlow: {
      type: "people",
      idField: "personID",
      storeNames: ["movie_credits", "series_credits"]
    }
  },
  Keywords: {
    items: () => keywordsArray,
    fields: ["name"],
    lookupFlow: {
      type: "keywords",
      idField: "keywordID",
      storeNames: ["movie_keywords", "series_keywords"]
    }
  }
};

/* Icon SVG/Template config */
const svgPlaceholderProfile = `<svg class="placeholder-profile"><use href="#placeholder_profile" class="low-opacity" /></svg>`;
/*
const svgPlaceholderPoster = `<svg class="placeholder-poster"><use href="#filmy_logo" class="low-opacity" /></svg>`;
const svgMaximize = `<svg class="icon-svg"><use href="#maximize"/></svg>`;
const svgMinimize = `<svg class="icon-svg"><use href="#minimize"/></svg>`;
const svgDownload = `<svg class="icon-svg"><use href="#download"/></svg>`;
const svgGrid = `<svg class="icon-svg"><use href="#grid"/></svg>`;
*/
const svgAward = `<svg class="icon-svg"><use href="#award"/></svg>`;
const svgSet =`<svg class="icon-svg"><use href="#set"/></svg>`;

let iconFragment = null;

const iconListConfig = {
  watchlist: {
    symbolId: "watchlist",
    label: "Watchlist",
    type: "core"
  },
  collection: {
    symbolId: "collection",
    label: "Collection",
    type: "core"
  },
  customlist: {
    symbolId: "list_custom",
    label: "List",
    type: "custom"
  },
  favourites: {
    symbolId: "favourites",
    label: "Favourites",
    type: "core"
  },
  watched: {
    symbolId: "eye",
    label: "Watched",
    type: "core"
  },
  unwatched: {
    symbolId: "eye_closed",
    label: "Not Watched",
    type: "core"
  }
};

// Global timer variable
let mouseTimer;
const inactivityTime = 3000; // 3 seconds inactivity

/** DOM elements */
const ratingsFilterButton = document.getElementById("ratings-filter-button");
const gridSizeSlider = document.getElementById('gridSizeSlider');
const gridSizeValue = document.getElementById('gridSizeValue');

const weightedRating = {
  minVotes: 1000,
  fallbackRating: 7.0,
  minVotesFallbackProduct: 1000 * 7.0,
  calculate(ratingStr, votesStr) {
    if (!ratingStr || !votesStr) return weightedRating.fallbackRating;
    
    const rating = Number(ratingStr);
    const votes = parseInt(votesStr.replace(/,/g, ''));
    
    if (isNaN(rating) || isNaN(votes) || votes === 0) return weightedRating.fallbackRating;
    
    return (votes * rating + weightedRating.minVotesFallbackProduct) / (votes + weightedRating.minVotes);
  }
};

/** DeBOUNCERS & THROTTLERS */
const debouncedApplyFilters = debounce(() => applyFiltersAndSearch(true, null, true), 70);
const throttledUpdateGridSizeLayout = throttle(updateGridSizeLayout, 16); // 16 = ~60fps
const throttledJumpToLetter = throttle(jumpToLetter, 400); // 400ms limit between jumps

// Cache for page size calculations
const pageSizeCache = {
  _cache: new Map(), // Using Map for better performance with complex keys
  _lastViewportWidth: window.innerWidth,
  _lastViewportHeight: window.innerHeight,

  // Get cached page size or calculate new one
  get(cardWidth) {
    // Invalidate cache if viewport has changed significantly
    this.invalidate();

    // Create a cache key based on card width and viewport dimensions
    const cacheKey = `${cardWidth}-${this._lastViewportWidth}-${this._lastViewportHeight}`;

    // Return cached value if available
    if (this._cache.has(cacheKey)) {
      const cachedResult = this._cache.get(cacheKey);
      // console.log(`Using cached page size: ${cachedResult} for width ${cardWidth}px`);
      return cachedResult;
    }

    // Calculate and cache the result
    const result = calculatePageSizeInternal(cardWidth);
    this._cache.set(cacheKey, result);
    return result;
  },

  // Check if viewport has changed significantly and invalidate cache if needed
  invalidate() {
    const viewportChanged =
      Math.abs(this._lastViewportWidth - window.innerWidth) > 10 ||
      Math.abs(this._lastViewportHeight - window.innerHeight) > 10;

    if (viewportChanged) {
      console.log("Viewport changed significantly, clearing page size cache");
      this._cache.clear();
      this._lastViewportWidth = window.innerWidth;
      this._lastViewportHeight = window.innerHeight;
    }
  },

  // Clear the cache manually
  clear() {
    this._cache.clear();
    console.log("Page size cache cleared");
  }
};

// API Key Manager using module pattern
const apiKeyManager = (function () {
  // Private variables
  const keys = {
    tmdb: '',
    omdb: []
  };

  // Get TMDB API key
  function getTmdbKey() {
    return keys.tmdb;
  }

  // Get all OMDB API keys
  function getOmdbKeys() {
    return [...keys.omdb]; // Return a copy to prevent modification
  }

  // Get the next available OMDB API key
  async function getNextOmdbKey() {
    try {
      if (!userSettings || !userSettings.username) {
        console.error("No user settings available");
        return keys.omdb.length > 0 ? keys.omdb[0] : null;
      }

      // Check if we need to reset the daily counts
      await checkAndResetApiCounts();

      // Get usage data from appSettings
      const apiUsage = userSettings.omdb_api_usage || {
        date: new Date().toISOString().split('T')[0],
        calls: keys.omdb.map(key => ({ key, count: 0 }))
      };

      // Find the first key that hasn't reached the limit
      for (const apiCall of apiUsage.calls) {
        if (apiCall.count < omdbRateLimit) {
          // console.log(`Using API key: ${apiCall.key}`);
          return apiCall.key;
        }
      }

      console.warn("All API keys have reached their daily limit.");
      return null;
    } catch (error) {
      console.error("Error getting next API key:", error);
      return keys.omdb.length > 0 ? keys.omdb[0] : null;
    }
  }

  // Initialize API keys from settings
  function initialize(settings) {
    if (!settings) return;

    keys.tmdb = settings.tmdb_apikey || '';
    keys.omdb = (settings.omdb_apikeys || []).filter(k => k && k.trim()); // Filter empty keys

    // Ensure API usage structure exists
    if (!settings.omdb_api_usage) {
      settings.omdb_api_usage = {
        date: new Date().toISOString().split('T')[0],
        calls: keys.omdb.map(key => ({ key, count: 0 }))
      };
    }

    // Remove any keys from usage tracking that are no longer in the keys list
    if (settings.omdb_api_usage && settings.omdb_api_usage.calls) {
      settings.omdb_api_usage.calls = settings.omdb_api_usage.calls.filter(
        call => keys.omdb.includes(call.key)
      );

      // Add any new keys that aren't being tracked yet
      const trackedKeys = settings.omdb_api_usage.calls.map(call => call.key);
      for (const key of keys.omdb) {
        if (!trackedKeys.includes(key)) {
          settings.omdb_api_usage.calls.push({ key, count: 0 });
        }
      }
    }
  }

  return {
    initialize,
    getTmdbKey,
    getOmdbKeys,
    getNextOmdbKey
  };
})();

/**
 * Check if the date has changed and reset API call counts if needed
 * @returns {Promise<void>}
 */
async function checkAndResetApiCounts() {
  if (!userSettings || !userSettings.username) return;

  const today = new Date().toISOString().split('T')[0];
  const apiUsage = userSettings.omdb_api_usage;

  // If no API usage data exists or the date has changed, reset counts
  if (!apiUsage || apiUsage.date !== today) {
    console.log("Resetting daily API call counts");

    userSettings.omdb_api_usage = {
      date: today,
      calls: apiKeyManager.getOmdbKeys().map(key => ({ key, count: 0 }))
    };

    // Save updated settings to database
    await saveUserSettingsToDB(userSettings);
  }
}

/**
 * Update the API call count for a specific key
 * @param {string} apiKey - The API key to update
 * @returns {Promise<void>}
 */
async function updateApiCallCount(apiKey) {
  if (!userSettings || !userSettings.username) return;

  // Make sure the usage structure exists
  if (!userSettings.omdb_api_usage) {
    await checkAndResetApiCounts();
  }

  const apiUsage = userSettings.omdb_api_usage;
  let keyFound = false;

  // Update the count for the specified key
  for (const apiCall of apiUsage.calls) {
    if (apiCall.key === apiKey) {
      apiCall.count += 1;
      keyFound = true;
      break;
    }
  }

  // If key not found in existing calls, add it
  if (!keyFound && apiKeyManager.getOmdbKeys().includes(apiKey)) {
    apiUsage.calls.push({ key: apiKey, count: 1 });
  }

  if (userSettings.username && typeof userSettings.username === 'string') {
    await saveUserSettingsToDB(userSettings, ['omdb_api_usage']);
  } else {
    console.error("Cannot save API usage: Invalid username", userSettings.username);
  }
}

/**
 * Set the call count for a specific API key
 * @param {string} apiKey - The API key to update
 * @param {number} count - The new call count
 * @returns {Promise<void>}
 */
async function setApiCallCount(apiKey, count) {
  if (!userSettings || !userSettings.username) return;

  // Make sure the usage structure exists
  if (!userSettings.omdb_api_usage) {
    await checkAndResetApiCounts();
  }

  const apiUsage = userSettings.omdb_api_usage;
  let keyFound = false;

  // Set the count for the specified key
  for (const apiCall of apiUsage.calls) {
    if (apiCall.key === apiKey) {
      apiCall.count = count;
      keyFound = true;
      break;
    }
  }

  // If key not found in existing calls, add it
  if (!keyFound && apiKeyManager.getOmdbKeys().includes(apiKey)) {
    apiUsage.calls.push({ key: apiKey, count });
  }

  // Save updated settings to database
  await saveUserSettingsToDB(userSettings, ['omdb_api_usage']);
}

// Background processing queue for people data
const backgroundPeopleQueue = {
  _queue: [],
  _isProcessing: false,
  _currentTask: null,
  _lastProcessedImdbID: null,
  _processedCount: 0,
  _totalProcessedCount: 0,
  _initialQueueSize: 0,
  _totalPeopleCount: 0,

  // Add a new task to the queue
  addTask(personIDs, imdbID) {
    this._queue.push({ personIDs, imdbID });

    // Update initial queue size and total people count
    if (!this._isProcessing) {
      this._initialQueueSize = this._queue.length;
      this._totalPeopleCount = 0; // Reset total count
      this._totalProcessedCount = 0; 
      // Calculate total from all tasks in queue
      for (let task of this._queue) {
        this._totalPeopleCount += task.personIDs.length;
      }
    } else {
      this._initialQueueSize = Math.max(this._initialQueueSize, this._queue.length);
      // Add to total people count
      this._totalPeopleCount += personIDs.length;
    }

    // Start processing if not already running
    if (!this._isProcessing) {
      this._processQueue();
    }

    // Update notification when adding to queue
    this._updateNotification();
  },

  // Process the queue
  async _processQueue() {
    this._isProcessing = true;

    try {
      this._processedCount = 0;
      this._updateNotification();

      while (this._queue.length > 0) {
        let task = this._queue.shift();
        this._lastProcessedImdbID = task.imdbID;

        this._updateNotification(task.imdbID, 0, task.personIDs.length);
        await this._processPeopleData(task.personIDs, task.imdbID);

        this._processedCount += task.personIDs.length;
        this._totalProcessedCount += task.personIDs.length;

        console.log(`Processed People count: ${this._processedCount}`);
      }
    } finally {
      this._showCompletionNotification();
      this._isProcessing = false;
      this._currentTask = null;
    }
  },

  async _processPeopleData(personIDs, imdbID) {
    try {
      this._currentTask = { personIDs, imdbID };
      console.log(`Background processing ${personIDs.length} people for ${imdbID}`);

      // First check which people already exist in the database
      const existingPeople = new Map();
      const transaction = db.transaction(['people'], 'readonly');
      const store = transaction.objectStore('people');

      // Get existing people in batches
      const batchSize = 50;
      for (let i = 0; i < personIDs.length; i += batchSize) {
        const batch = personIDs.slice(i, i + batchSize);
        await Promise.all(batch.map(async (id) => {
          try {
            const record = await new Promise((resolve, reject) => {
              const request = store.get(id);
              request.onsuccess = () => resolve(request.result);
              request.onerror = (e) => reject(e.target.error);
            });

            if (record) {
              existingPeople.set(id, record);
            }
          } catch (e) {
            console.warn(`Error checking person ${id}:`, e);
          }
        }));
      }

      // Filter out people who are already in the database and not stale
      const now = Date.now();
      const maxAge = 14 * 24 * 60 * 60 * 1000; // 14 days in ms
      const personIDsToProcess = personIDs.filter(pid => {
        const record = existingPeople.get(pid);
        return !record || !record.timestamp || (now - record.timestamp > maxAge);
      });

      console.log(`_processPeopleData: Filtered out ${personIDs.length - personIDsToProcess.length} existing/fresh records`);

      if (personIDsToProcess.length > 0) {
        let peopleDetails = await queryTMDB(
          personIDsToProcess.map(id => ({ tmdbID: id })),
          'details',
          'person',
          true
        );

        if (peopleDetails?.length > 0) {
          let transformedPeople = peopleDetails.map(({ id, ...rest }) => ({
            ...rest,
            personID: id,
            timestamp: Date.now(),
          }));
          await saveToDatabase(db, transformedPeople, 'people');
        }
      }
    } catch (error) {
      console.error("Background processing failed:", error);
    } finally {
      this._currentTask = null;
    }
  },

  _updateProgress(successCount) {
    if (this._currentTask) {
      this._updateNotification(this._currentTask.imdbID, successCount, this._currentTask.personIDs.length);
    }
  },

  _updateNotification(currentImdbID = null, processed = 0, total = 0) {
    // Single media card refresh
    if (this._initialQueueSize === 1) {
      let imdbID = currentImdbID || this._currentTask?.imdbID || this._lastProcessedImdbID || "unknown";
      showNotification(`Fetching ${processed}/${total} people for ${imdbID}...`, true, true);
      return;
    }

    // Queue case
    let queuePosition = this._initialQueueSize - this._queue.length;
    let queueText = `Queue: ${queuePosition}/${this._initialQueueSize}`;

    // Calculate total people in remaining queue
    let remainingPeople = 0;
    if (Array.isArray(this._queue)) {
      remainingPeople = this._queue.reduce((acc, task) => acc + task.personIDs.length, 0);
    }

    // Add current batch's total to the count
    let totalPeopleToProcess = this._totalPeopleCount;
    if (total > 0) {
      totalPeopleToProcess = Math.max(totalPeopleToProcess, this._totalProcessedCount + remainingPeople + total);
    }

    let peopleText = `People processed: ${this._totalProcessedCount}/${totalPeopleToProcess}`;
    let currentText = currentImdbID ? `Fetching ${processed}/${total} records for ${currentImdbID}...` : '';

    showNotification(`${queueText} | ${peopleText} | ${currentText}`, true, true);
  },

  _showCompletionNotification() {
    if (this._initialQueueSize === 1) {
      let imdbID = this._lastProcessedImdbID || "unknown";
      showNotification(`Completed processing people for ${imdbID}.`, false, false);
    } else {
      showNotification(`Queue complete: ${this._initialQueueSize}/${this._initialQueueSize}. Total people processed: ${this._totalProcessedCount}.`, false, false);
    }

    setTimeout(() => {
      removeNotification(n => n.isBackgroundTask);
    }, 3000);
  },

  getStatus() {
    return {
      queueLength: this._queue.length,
      isProcessing: this._isProcessing,
      currentTask: this._currentTask,
      processedCount: this._processedCount,
      totalProcessedCount: this._totalProcessedCount,
      initialQueueSize: this._initialQueueSize,
      totalPeopleCount: this._totalPeopleCount
    };
  },
};

/**
 * Enriches company or network arrays with SVG information
 * @param {Array} items - Array of production companies or networks
 * @param {string} type - Either 'company' or 'network'
 */
async function fetchSvgInfo(items, type) {
  // Process in batches to avoid too many simultaneous requests
  const batchSize = 5;

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);

    // Create promises for all items in the batch
    const promises = batch.map(async (item) => {
      try {
        const url = `${tmdbApiBaseUrl}${type}/${item.id}/images?api_key=${apiKeyManager.getTmdbKey()}`;
        const response = await fetch(url);

        if (!response.ok) {
          console.warn(`Failed to fetch ${type} images for ID ${item.id}: ${response.status}`);
          return;
        }

        const imageData = await response.json();

        // Check if SVG logo exists
        if (imageData && imageData.logos && imageData.logos.length > 0) {
          const svgLogo = imageData.logos.find(logo => logo.file_type === ".svg");

          if (svgLogo) {
            // Add SVG info to the item
            item.file_type = ".svg";

            // Update logo_path if the SVG path is different from the current path
            if (item.logo_path && item.logo_path !== svgLogo.file_path) {
              item.logo_path = svgLogo.file_path;
              // console.log(`Updated logo path for ${type} ${item.name} (ID: ${item.id}) to SVG: ${svgLogo.file_path}`);
            }
            // console.log(`Found SVG logo for ${type} ${item.name} (ID: ${item.id})`);
          }
        }
      } catch (error) {
        console.error(`Error fetching ${type} images for ID ${item.id}:`, error);
      }
    });

    // Wait for all promises in this batch to resolve
    await Promise.all(promises);

    // Add a small delay between batches to avoid rate limiting
    if (i + batchSize < items.length) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
}

// Function to calculate proper date range for Changes API (max 14 days)
function calculateChangesDateRange(lastUpdateTimestamp) {
  const currentDate = new Date();
  const lastUpdate = new Date(lastUpdateTimestamp);

  // Calculate days since last update
  const daysSinceLastUpdate = Math.floor((currentDate - lastUpdate) / (1000 * 60 * 60 * 24));

  // Fix: only consider > 14 days as exceeding max range
  const exceededMaxRange = daysSinceLastUpdate > 14;

  // Determine start date - use lastUpdate unless it's more than 14 days ago
  const startDate = exceededMaxRange
    ? new Date(currentDate.getTime() - (14 * 24 * 60 * 60 * 1000))
    : lastUpdate;

  // Format dates as YYYY-MM-DD for the API
  const startDateStr = startDate.toISOString().split('T')[0];
  const endDateStr = currentDate.toISOString().split('T')[0];

  return {
    startDate: startDateStr,
    endDate: endDateStr,
    exceededMaxRange
  };
}

/**
 * add & refresh titles to the database
 * @param {*} db 
 * @param {*} param1 
 * @returns 
 */
async function addTitle(db, { title = null, year = null, imdb_ID = null, category = null, tmdbID = null, isRefresh = false } = {}) {
  // Detect if this is part of a batch process
  const isBatchProcess = !title && imdb_ID && !isRefresh;

  let selectedMedia;
  const mediaType = category === 'series' ? 'tv' : (category === 'all' ? 'multi' : 'movie');
  try {
    // Handle refresh operations
    if (isRefresh) {
      // For refresh operations, we should already have tmdbID
      if (!tmdbID) {
        throw new Error("Cannot refresh: Missing tmdbID parameter");
      }

      // Convert tmdbID to number once
      const tmdbIDNum = Number(tmdbID);

      // Get title and check for changes
      const storeName = mediaType === 'tv' ? 'series' : 'movies';
      const existingRecord = await getFromDatabase(db, storeName, imdb_ID);

      // Get title for notifications
      title = existingRecord ? (existingRecord.Title || existingRecord.name || `${imdb_ID} No Title`) : `${imdb_ID} No Title`;

      // Check for missing data in related stores
      const storePrefix = mediaType === 'tv' ? 'series' : 'movie';
      const missingDataStores = [];

      // Define stores to check
      const relatedStores = [
        `${storePrefix}_credits`,
        `${storePrefix}_details`,
        `${storePrefix}_genres`,
      //  `${storePrefix}_keywords` // these can be empty
      ];

      // Add release_dates check for movies only
      if (mediaType === 'movie') {
        relatedStores.push('release_dates');
      }

      // Check each store for data
      for (const storeName of relatedStores) {
        try {
          // For stores where tmdbID is the primary key (like movie_details)
          if (storeName.endsWith('_details')) {
            const record = await getFromDatabase(db, storeName, tmdbIDNum);
            if (!record) {
              missingDataStores.push(storeName);
            }
          }
          // For all other stores
          else {
            const transaction = db.transaction([storeName], 'readonly');
            const store = transaction.objectStore(storeName);

            let hasData = false;
            if (store.indexNames.contains('tmdbID')) {
              // Use index to find records with this tmdbID
              const index = store.index('tmdbID');
              const request = index.count(IDBKeyRange.only(tmdbIDNum));

              hasData = await new Promise((resolve, reject) => {
                request.onsuccess = () => resolve(request.result > 0);
                request.onerror = (e) => reject(e.target.error);
              });
            }

            if (!hasData) {
              missingDataStores.push(storeName);
            }
          }
        } catch (error) {
          console.warn(`Unable to check ${storeName}: ${error.message}`);
          missingDataStores.push(storeName);
        }
      }

      // If any data is missing, force a refresh regardless of changes
      if (missingDataStores.length > 0) {
        showNotification(`Missing data for "${title}" (${missingDataStores.join(', ')}). Performing full refresh...`, true);
        console.log(`Missing data for "${title}" (${missingDataStores.join(', ')})`);
        // Continue with refresh - don't return or check for changes
      }
      // Otherwise check for changes since last update
      else if (existingRecord && existingRecord.timestamp) {
        // Calculate proper date range with 14-day maximum
        const { startDate, endDate, exceededMaxRange } = calculateChangesDateRange(existingRecord.timestamp);

        if (exceededMaxRange) {
          // Always refresh if it's been more than 14 days
          showNotification(`Last update was more than 14 days ago. Refreshing "${title}"...`, true);
          // No changes check - just continue to the refresh code below
        }
        else {
          // Only check for changes if within the 14-day window
          try {
            // Check for changes in the allowed date range
            const changesUrl = `${tmdbApiBaseUrl}${mediaType}/${tmdbID}/changes?api_key=${apiKeyManager.getTmdbKey()}&start_date=${startDate}&end_date=${endDate}`;
            const changesResponse = await fetch(changesUrl);

            if (!changesResponse.ok) {
              console.warn(`TMDB changes check failed: ${changesResponse.status}. Proceeding with refresh.`);
              showNotification(`Checking for changes failed, refreshing "${title}" anyway...`, true);
              // Continue with refresh instead of throwing
            } else {
              const changesData = await changesResponse.json();

              // Skip refresh if no changes detected and no missing data
              if (!changesData.changes || changesData.changes.length === 0) {
                showNotification(`No changes detected for "${title}" since last update`, false);
                return; // Only return here if there are no changes AND no missing data
              }

              showNotification(`Changes detected for "${title}", refreshing...`, true);
            }
          } catch (error) {
            console.warn(`Error checking for changes: ${error.message}. Proceeding with refresh.`);
            showNotification(`Checking for changes failed, refreshing "${title}" anyway...`, true);
            // Continue with refresh
          }
        }
      } else {
        // If we can't check for changes, just refresh
        showNotification(`Can't check for changes. Refreshing "${title}"...`, true);
      }
    }
    // Handle new title additions
    else {
      if (!imdb_ID && !title) {
        throw new Error("Must provide either imdbID or title");
      }

      if (imdb_ID) {
        const lookupUrl = `${tmdbApiBaseUrl}find/${imdb_ID}?api_key=${apiKeyManager.getTmdbKey()}&external_source=imdb_id`;
        const lookupResponse = await fetch(lookupUrl);
        if (!lookupResponse.ok) throw new Error(`TMDB lookup failed: ${lookupResponse.status}`);
        const lookupData = await lookupResponse.json();
        selectedMedia = lookupData.movie_results?.[0] || lookupData.tv_results?.[0];
        if (!selectedMedia) {
          showNotification(`No TMDB entry found for IMDb ID ${imdb_ID}`, false);
          return false; // Return false to indicate failure in batch mode
        }
      } else {
        const searchEndpoint = mediaType === 'multi' ? 'search/multi' : mediaType === 'tv' ? 'search/tv' : 'search/movie';
        const searchUrl = `${tmdbApiBaseUrl}${searchEndpoint}?api_key=${apiKeyManager.getTmdbKey()}&query=${encodeURIComponent(title)}${year ? `&year=${year}` : ""}`;
        const searchResponse = await fetch(searchUrl);
        if (!searchResponse.ok) throw new Error(`TMDB search failed: ${searchResponse.status}`);
        const searchData = await searchResponse.json();
        const results = searchData.results || [];
        if (!results.length) {
          showNotification(`No results found for "${title}"${year ? ` (${year})` : ""}.`, false);
          return false; // Return false to indicate title search failure
        }
        // Handle result selection
        selectedMedia = results[0];
        if (results.length > 1) {
          selectedMedia = await showSelectionPopup(
            results.map(item => ({
              id: item.id,
              title: item.title || item.name,
              release_date: item.release_date || item.first_air_date,
              overview: item.overview,
              poster_path: item.poster_path,
              media_type: item.media_type
            })),
            title,
            year
          );
          if (!selectedMedia) {
            showNotification("Search cancelled by user.", false);
            return false; // Return false to indicate user aborted the operation
          }
        }
      }

      tmdbID = selectedMedia.id;
      title = selectedMedia.title || selectedMedia.name;
    }

    // Get the media type from selectedMedia if available, otherwise use the category
    const selectedMediaType = (selectedMedia?.media_type) || (category === 'series' ? 'tv' : 'movie');

    if (!isBatchProcess) {
      showNotification(`Fetching details for "${title}"...`, true);
    }

    const detailAppend =
      "credits,keywords,images,recommendations,similar,videos," +
      (selectedMediaType === "movie"
        ? "release_dates"
        : "content_ratings,external_ids,aggregate_credits");

    const detailsUrl = `${tmdbApiBaseUrl}${selectedMediaType}/${tmdbID}?api_key=${apiKeyManager.getTmdbKey()}&append_to_response=${detailAppend}`;
    const detailsResponse = await fetch(detailsUrl);
    if (!detailsResponse.ok) throw new Error(`TMDB details fetch failed: ${detailsResponse.status}`);

    let tmdbData = await detailsResponse.json();
    tmdbData.genre_ids = tmdbData.genres?.map(genre => genre.id) || [];
    tmdbData = renameAllIDs(tmdbData, endpointContextMap);
    tmdbData.media_type = selectedMediaType;

    // Check for SVG logos for production companies
    if (tmdbData.production_companies && tmdbData.production_companies.length > 0) {
      await fetchSvgInfo(tmdbData.production_companies, 'company');
    }

    // Check for SVG logos for networks (TV shows only)
    if (selectedMediaType === 'tv' && tmdbData.networks && tmdbData.networks.length > 0) {
      await fetchSvgInfo(tmdbData.networks, 'network');
    }

    const storePrefix = selectedMediaType === "tv" ? "series" : "movie";
    const endpointDataMap = {};

    if (tmdbData.keywords?.results || tmdbData.keywords?.keywords) {
      const keywordsArray = tmdbData.keywords.results || tmdbData.keywords.keywords;
      endpointDataMap[`${storePrefix}_keywords`] = transformKeywords(keywordsArray, tmdbID);
      endpointDataMap["keywords"] = keywordsArray;
    }

    // Extract personIDs for background processing
    let personIDs = [];

    // Process credits using aggregate_credits for TV shows if available
    if (selectedMediaType === 'tv' && tmdbData.aggregate_credits) {
      // Get top 6 directors by episode count
      tmdbData.directors = extractTopCrew(
        tmdbData.aggregate_credits,
        ['Director'],
        false // Job-based filtering
      );
      // Get top 6 writers by episode count
      tmdbData.writers = extractTopCrew(
        tmdbData.aggregate_credits,
        ['Writer', 'Screenplay', 'Story'],
        false // Job-based filtering
      );

      tmdbData.topCast = extractCast(tmdbData.aggregate_credits, 6);

      endpointDataMap[`${storePrefix}_credits`] = transformCredits(tmdbData.aggregate_credits, tmdbID);

      personIDs = [...new Set([
        ...tmdbData.aggregate_credits.cast.map(p => p.personID),
        ...tmdbData.aggregate_credits.crew.map(p => p.personID)
      ])];

    } else if (tmdbData.credits) {
      // Original movie handling remains unchanged
      tmdbData.directors = extractCrew(tmdbData.credits, "Director");
      tmdbData.writers = extractCrew(tmdbData.credits, "Writing", true);
      tmdbData.topCast = extractCast(tmdbData.credits, 5);

      endpointDataMap[`${storePrefix}_credits`] = transformCredits(tmdbData.credits, tmdbID);

      personIDs = [...new Set([
        ...tmdbData.credits.cast.map(p => p.personID),
        ...tmdbData.credits.crew.map(p => p.personID)
      ])];

    }
    if (tmdbData.genre_ids) {
      endpointDataMap[`${storePrefix}_genres`] = tmdbData.genre_ids.map(genreID => ({ tmdbID, genreID }));
    }

    endpointDataMap[`${storePrefix}_details`] = {
      ...extractCoreDetails(tmdbData),
      content_ratings: [] // Initialize empty array
    };
    if (selectedMediaType === 'movie' && tmdbData.release_dates) {
      endpointDataMap.release_dates = transformReleaseDates(tmdbData.release_dates, tmdbID, 'movie');
    } else if (selectedMediaType === 'tv' && tmdbData.content_ratings) {
      endpointDataMap[`${storePrefix}_details`].content_ratings = transformReleaseDates(
        tmdbData.content_ratings,
        tmdbID, 'tv'
      );
    }

    const imdbID = tmdbData.imdb_id || (selectedMediaType === 'tv' ? tmdbData.external_ids?.imdb_id : null) || `tmdb-${tmdbID}`;
    let omdbData = imdbID.startsWith('tt') ? await queryOMDB(imdbID) : null;
    const storeName = selectedMediaType === 'tv' ? 'series' : 'movies';
    const freshData = mergeTMDbAndOMDb(tmdbData, omdbData);
    const existingRecord = await getFromDatabase(db, storeName, imdbID);
    const mergedRecord = existingRecord ? mergeEndpointData(existingRecord, freshData) : freshData;

    endpointDataMap[storeName] = { imdbID, ...mergedRecord };

    if (mergedRecord.Ratings) {
      endpointDataMap.ratings = mergedRecord.Ratings.map(rating => ({
        imdbID,
        Source: rating.Source,
        Value: rating.Value.replace(/\/\d+$/, '')
      }));
    }

    const failedEndpoints = [];
    for (const [storeName, data] of Object.entries(endpointDataMap)) {
      try {
        if (data && Object.keys(data).length > 0) {
          await saveToDatabase(db, data, storeName);
        }
      } catch {
        failedEndpoints.push(storeName);
      }
    }

    // Queue people processing in the background
    if (personIDs.length > 0) {
      const peopleMap = await getPeopleInfo(personIDs);

      const now = Date.now();
      // 14 days in ms
      const maxAge = 14 * 24 * 60 * 60 * 1000;

      // Only queue personIDs that are missing or stale
      const personIDsToProcess = personIDs.filter(pid => {
        const rec = peopleMap.get(pid);
        return !rec || !rec.timestamp || (now - rec.timestamp) > maxAge;
      });

      if (personIDsToProcess.length > 0) {
        backgroundPeopleQueue.addTask(personIDsToProcess, imdbID);
      }
    }

    // If this is part of a batch process, use a more concise notification
    if (isBatchProcess) {
      if (failedEndpoints.length === 0) {
        console.log(`Added "${mergedRecord.Title || mergedRecord.name}" successfully!`);
      } else {
        console.warn(`Partial save failed for: ${failedEndpoints.join(', ')}`);
      }
    } else {
      // Standard notification for interactive use
      if (failedEndpoints.length === 0) {
        const successMessage = `"${mergedRecord.Title || mergedRecord.name}" ${existingRecord ? 'updated' : 'added'} successfully!`;
        showNotification(`✓ ${successMessage}`, false);
        console.log(successMessage);
        // Create or update the media card in the grid
        await updateMediaCard(mergedRecord.imdbID, selectedMediaType);
      } else {
        showNotification(`❌ Partial save failed for: ${failedEndpoints.join(', ')}`, false);
      }
    }

    return mergedRecord.imdbID;

  } catch (error) {
    // If this is part of a batch process, log the error but don't throw
    if (isBatchProcess) {
      console.error(`Error processing ${imdb_ID}:`, error);
      return null; // Return false to indicate failure
    } else {
      // Standard error handling for interactive use
      console.error("Error adding title:", error);
      showNotification(`❌ Error processing ${mediaType === "tv" ? "series" : "movie"}: ${error.message}`, false);
      return null;
    }
  }
}

/**
 * Updates or creates a media card in the grid
 * @param {string} imdbID - The IMDb ID of the media
 * @param {string} mediaType - The type of media (movie or tv)
 * @returns {Promise<boolean>} - Success status
 */
async function updateMediaCard(imdbID, mediaType) {
  try {
    // Update cache with fresh data from IndexedDB
    const updatedRecord = await updateCache(imdbID, db, mediaType);
    if (!updatedRecord) {
      console.warn(`Cache update failed for IMDb ID: ${imdbID}`);
      return false;
    }

    // Check if the card already exists in the grid
    const existingCard = document.querySelector(`.media-card[data-imdbid="${imdbID}"]`);

    // If card doesn't exist, create a new one
    if (!existingCard) {
      console.log(`Creating new card for IMDb ID: ${imdbID}`);

      convertYearToNumber(updatedRecord);

      // Render the new card
      renderCard([updatedRecord], "prepend");

      // Update global state
      filteredRecords.push(updatedRecord);
      totalRecords += 1;

      // Update UI filters
      updateActiveYears();
      populateCountryDropdown(Array.from(mediaCache.values())); 
      initializeGenreFilter();
      initializeYearSlider();
      updateMatchCount();

      return true;
    }

    // Otherwise, update the existing card
    console.log(`Updating existing card for IMDb ID: ${imdbID}`);

    // Check if detail view is open for this media
    if (isFromDetailView()) {
      const popup = document.getElementById("popup");
      const detailView = popup.querySelector(`.detail-view[data-imdbid="${imdbID}"]`);

      if (detailView) {
        console.log(`Refreshing detail view for IMDb ID: ${imdbID}`);

        // Create a mock event and element to pass to handleDetailView
        const mockEvent = { preventDefault: () => {}, stopPropagation: () => {} };
        const mockElement = { closest: () => ({ dataset: { imdbid: imdbID, mediaType } }) };

        // Call handleDetailView with our mock event and element
        await handleDetailView(mockEvent, mockElement);

        console.log(`Detail view refreshed for IMDb ID: ${imdbID}`);
      }
    }

    // Refresh the tooltip element
    const tooltip = existingCard.querySelector('.tooltip-text');
    if (tooltip) {
      const newContent = generateTooltip(updatedRecord);
      tooltip.innerHTML = newContent;
      void tooltip.offsetHeight; // Force reflow
    }

    // Refresh the media container element with the new poster
    const mediaContainer = existingCard.querySelector('.media-container');
    const placeholderPoster = existingCard.querySelector('.placeholder-poster');
    
    if (mediaContainer || placeholderPoster) {
      const containerElement = mediaContainer || placeholderPoster;
      
      if (updatedRecord.Poster && updatedRecord.Poster !== "N/A") {
        // Replace the container with a media-container with an image
        const newContainer = document.createElement('div');
        newContainer.className = 'media-container';
        newContainer.innerHTML = `<img src="${buildTMDbImageUrl(updatedRecord.Poster, "poster")}" alt="${updatedRecord.Title}" loading="lazy">`;
        
        // Replace the old container with the new one
        containerElement.parentNode.replaceChild(newContainer, containerElement);
      } else {
        // Replace with a placeholder poster
        const newContainer = document.createElement('div');
        newContainer.className = 'placeholder-poster';
        newContainer.innerHTML = createPlaceholderSVG(300, 450, 'poster');
        
        // Replace the old container with the new one
        containerElement.parentNode.replaceChild(newContainer, containerElement);
      }
    }

    // Update the record in filteredRecords array if it exists there
    const recordIndex = filteredRecords.findIndex(record => record.imdbID === imdbID);
    if (recordIndex !== -1) {
      filteredRecords[recordIndex] = updatedRecord;
    }

    return true;
  } catch (error) {
    console.error(`Error updating card for IMDb ID ${imdbID}:`, error);
    return false;
  }
}

function renameIdsDynamically(data, context = null) {
  if (Array.isArray(data)) {
    return data.map((item) => renameIdsDynamically(item, context));
  } else if (data && typeof data === "object") {
    const renamedData = {};

    for (const [key, value] of Object.entries(data)) {
      if (key === "id") {
        if (context === "movie" || context === "tv" || context === "similar" || context === "recommendations") {
          renamedData["tmdbID"] = value; // Rename movie and TV series IDs
        } else if (context === "person") {
          renamedData["personID"] = value;
        } else if (context === "keyword") {
          renamedData["keywordID"] = value;
        } else {
          renamedData[key] = value;
        }
      } else {
        renamedData[key] = renameIdsDynamically(value, context);
      }
    }

    return renamedData;
  }

  return data;
}

function renameAllIDs(tmdbData, endpointContextMap) {
  const { id, ...rest } = tmdbData;

  // Rename top-level id to tmdbID for the main movie object
  const renamedData = { tmdbID: id, ...rest };

  // Iterate through all keys in the data
  for (const [endpoint, data] of Object.entries(renamedData)) {
    const context = endpointContextMap[endpoint]; // Get context from mapping

    if (context) {
      // Apply renaming function based on context
      renamedData[endpoint] = renameIdsDynamically(data, context);
    }
  }

  return renamedData;
}

/**
 * Extract core movie details from TMDB API response.
 * Removes appended endpoint data.
 * @param {Object} tmdbData - The full TMDB API response with appended endpoints.
 * @returns {Object} - The core movie details.
 */
function extractCoreDetails(tmdbData) {
  // Destructure and exclude appended endpoint fields
  const {
    credits,
    aggregate_credits,
    keywords,
    release_dates,
    external_ids,
    content_ratings,
    writers,
    directors,
    topCast,
    ...coreDetails // Everything else is part of core details
  } = tmdbData;

  return coreDetails; // Return only the core details plus page 1 of images, videos, recommendations, similar
}

function mergeTMDbAndOMDb(tmdbData, omdbData = null, releaseDatesData = null) {
  if (!tmdbData) {
    console.error("TMDB Data is missing.");
    return null;
  }

  // Helper functions
  const splitAndTrim = (str) => str && str !== "N/A" ? str.split(",").map(s => s.trim()) : [];
  const extractYear = (dateStr) =>
    dateStr
      ? (dateStr.split(/[-–-]/)[0].match(/\d{4}/)?.[0] || "N/A")
      : "N/A";
  const getNames = (arr) => arr?.map(item => item.name) || [];

  const isTV = tmdbData.media_type === 'tv';

  // Cache frequently accessed values
  const tmdbYear = isTV ? extractYear(tmdbData.first_air_date) : extractYear(tmdbData.release_date);
  const omdbYear = omdbData?.Year || "N/A";
  const hasTrailingDash = omdbYear !== "N/A" && /[-–-]\s*$/.test(omdbYear);

  // Determine title and basic metadata
  const title = omdbData?.Title || (isTV ? tmdbData.name : tmdbData.title) || "N/A";

  // Process certification/rating - simplified logic
  let rated = "N/A";
  if (isTV && tmdbData.content_ratings?.results) {
    rated = tmdbData.content_ratings.results.find(r => r.iso_3166_1 === "US")?.rating || "N/A";
  } else if (!isTV && releaseDatesData?.results) {
    rated = releaseDatesData.results.find(entry => entry.iso_3166_1 === "US")?.release_dates?.[0]?.certification || "N/A";
  }

  // Create the merged object
  const merged = {
    // Core fields - direct assignments for clarity
    Title: title,
    Year: omdbYear !== "N/A" && !hasTrailingDash ? omdbYear : tmdbYear,
    Rated: omdbData?.Rated || rated,
    Released: isTV ? (tmdbData.first_air_date || "N/A") : (omdbData?.Released || tmdbData.release_date || "N/A"),
    Runtime: isTV
      ? (tmdbData.episode_run_time?.[0] ? `${tmdbData.episode_run_time[0]} min` : null)
      : (omdbData?.Runtime || (tmdbData.runtime ? `${tmdbData.runtime} min` : null)),

    // Common metadata - use helpers for string processing
    Genre: omdbData?.Genre ? splitAndTrim(omdbData.Genre) : getNames(tmdbData.genres),
    Director: omdbData?.Director ? splitAndTrim(omdbData.Director) : (tmdbData.directors || []),
    Writer: omdbData?.Writer ? splitAndTrim(omdbData.Writer) : (tmdbData.writers || []),
    Actors: omdbData?.Actors ? splitAndTrim(omdbData.Actors) : (tmdbData.topCast || []),

    // Location metadata
    Country: omdbData?.Country && omdbData.Country !== "N/A"
      ? splitAndTrim(omdbData.Country)
      : getNames(tmdbData.production_countries),

    // Descriptive fields - direct assignments
    Plot: (omdbData?.Plot && omdbData.Plot !== "N/A") ? omdbData.Plot : (tmdbData.overview || "N/A"),
    Language: omdbData?.Language && omdbData.Language !== "N/A"
      ? splitAndTrim(omdbData.Language)
      : (tmdbData.spoken_languages?.map(l => l.english_name || l.name) || []),

    // Consolidated ratings array
    Ratings: [
      ...(omdbData?.Ratings || []),
      ...(tmdbData.vote_average ? [{
        Source: "TMDB",
        Value: `${tmdbData.vote_average.toFixed(1)}/10`
      }] : [])
    ],

    // Remaining fields - direct assignments for clarity
    Metascore: omdbData?.Metascore || "N/A",
    BoxOffice: tmdbData.revenue ? `$${tmdbData.revenue.toLocaleString()}` : (omdbData?.BoxOffice || "N/A"),
    Production: omdbData?.Production || (tmdbData.production_companies ? getNames(tmdbData.production_companies).join(", ") : "N/A"),
    Poster: tmdbData.poster_path || null,
    Website: omdbData?.Website || tmdbData.homepage || "N/A",

    // Technical fields
    imdbRating: omdbData?.imdbRating || "N/A",
    imdbVotes: omdbData?.imdbVotes || "N/A",
    imdbID: tmdbData.imdb_id || tmdbData.external_ids?.imdb_id || `tmdb-${tmdbData.tmdbID}`,
    tmdbID: tmdbData.tmdbID,
    dataTitle: `${normalizeString(title)} ${omdbYear !== "N/A" ? omdbYear : tmdbYear}`.trim(),

    // Technical metadata - direct assignments
    adult: !!tmdbData.adult,
    backdrop_path: tmdbData.backdrop_path || null,
    genre_ids: tmdbData.genre_ids || [],
    origin_country: tmdbData.origin_country || [],
    original_title: isTV ? tmdbData.original_name : tmdbData.original_title,
    popularity: tmdbData.popularity || null,
    video: !!tmdbData.video,
    vote_average: tmdbData.vote_average || null,
    vote_count: tmdbData.vote_count || null
  };

  // Add TV-specific fields conditionally to avoid undefined properties
  if (isTV) {
    Object.assign(merged, {
      number_of_seasons: tmdbData.number_of_seasons || 0,
      number_of_episodes: tmdbData.number_of_episodes || 0,
      status: tmdbData.status || "N/A",
      seasons: tmdbData.seasons || [],
      networks: tmdbData.networks ? getNames(tmdbData.networks) : []
    });
  }

  return merged;
}

function transformReleaseDates(responseData, tmdbID, mediaType) {
  if (!tmdbID || !responseData?.results) return [];

  if (mediaType === 'movie') {
    return responseData.results.flatMap(result =>
      (result.release_dates || []).map(release => ({
        tmdbID,
        countryCode: result.iso_3166_1,
        certification: release.certification,
        release_date: release.release_date,
        note: release.note || "", // Include note field
        type: release.type || null, // Include type field
      }))
    );
  }

  if (mediaType === 'tv') {
    // Return just the array of certification objects without tmdbID
    return responseData.results
      .filter(r => r.rating && r.rating !== "N/A")
      .map(({ iso_3166_1, rating }) => ({
        country: iso_3166_1,
        certification: rating
      }));
  }

  return [];
}

/**
 * Transform flattened keywords into "{store}_keywords" structure.
 * @param {Array} keywords - The flattened keywords array (already contains keywordID and name).
 * @param {number} tmdbID - The TMDB movie ID.
 * @returns {Array} - An array of objects for the "{store}_keywords" store.
 */
function transformKeywords(keywords, tmdbID) {
  // Validate inputs
  if (!Array.isArray(keywords)) {
    console.error("Invalid keywords data:", keywords);
    return [];
  }

  if (!tmdbID) {
    console.error("Missing TMDB ID:", tmdbID);
    return [];
  }

  // Transform keywords into "movie_keywords" structure
  const mediaKeywords = keywords.map((keyword) => ({
    tmdbID, // Associate the movie's TMDB ID
    keywordID: keyword.keywordID || null, // Use keywordID from the existing flattened structure
  }));

  return mediaKeywords;
}

function transformCredits(credits, tmdbID) {
  const flattenedCredits = [];

  // Validate inputs
  if (!credits || !Array.isArray(credits.cast) || !Array.isArray(credits.crew)) {
    console.error("Invalid credits data:", credits);
    return flattenedCredits;
  }

  // Process cast members
  credits.cast.forEach((member) => {
    if (member.roles && Array.isArray(member.roles)) {
      // Handle TV structure with roles array
      member.roles.forEach((role) => {
        flattenedCredits.push({
          tmdbID,
          personID: member.personID != undefined ? member.personID : null,
          name: member.name || null,
          role: "cast",
          character: role.character || null,
          order: member.order != undefined ? member.order : null,
          cast_id: member.cast_id != undefined ? member.cast_id : null,
          credit_id: role.credit_id || `${tmdbID}-${member.id}-cast`,
          profile_path: member.profile_path || null,
          episode_count: role.episode_count || null, // Specific to TV
        });
      });
    } else {
      // Handle movie structure
      flattenedCredits.push({
        tmdbID,
        personID: member.personID != undefined ? member.personID : null,
        name: member.name || null,
        role: "cast",
        character: member.character || null,
        order: member.order != undefined ? member.order : null,
        cast_id: member.cast_id != undefined ? member.cast_id : null,
        credit_id: member.credit_id || `${tmdbID}-${member.id}-cast`,
        profile_path: member.profile_path || null,
      });
    }
  });

  // Process crew members
  credits.crew.forEach((member) => {
    if (member.jobs && Array.isArray(member.jobs)) {
      // Handle TV structure with jobs array
      member.jobs.forEach((job) => {
        flattenedCredits.push({
          tmdbID,
          personID: member.personID != undefined ? member.personID : null,
          name: member.name || null,
          role: "crew",
          job: job.job || null,
          department: member.department || null,
          credit_id: job.credit_id || `${tmdbID}-${member.id}-${job.job}`,
          profile_path: member.profile_path || null,
          episode_count: job.episode_count || null, // Specific to TV
        });
      });
    } else {
      // Handle movie structure
      flattenedCredits.push({
        tmdbID,
        personID: member.personID != undefined ? member.personID : null,
        name: member.name || null,
        role: "crew",
        job: member.job || null,
        department: member.department || null,
        credit_id: member.credit_id || `${tmdbID}-${member.id}-crew`,
        profile_path: member.profile_path || null,
      });
    }
  });

  return flattenedCredits;
}

/**
 * Extract crew members by job title or department.
 * @param {Object} credits - The credits object from TMDB.
 * @param {string|Array<string>} filter - The job title(s) or department(s) to filter by.
 * @param {boolean} isDepartment - If true, filter by department instead of job title.
 * @returns {Array<string>} - An array of crew member names.
 */
function extractCrew(credits, filter, isDepartment = false) {
  if (!credits || !credits.crew) return [];
  const filters = Array.isArray(filter) ? filter : [filter];

  return credits.crew
    .filter(person => {
      // Handle TV structure with nested jobs array
      if (person.jobs) {
        return person.jobs.some(job =>
          isDepartment ?
            filters.includes(job.department) :
            filters.includes(job.job)
        );
      }
      // Handle movie structure with flat job/department
      return isDepartment ?
        filters.includes(person.department) :
        filters.includes(person.job);
    })
    .map(person => person.name);
}

// Modified extractCrew to handle episode counts and TV job arrays
function extractTopCrew(credits, filter, isDepartment = false, limit = 6) {
  if (!credits?.crew) return [];

  return credits.crew
    .filter(person => {
      if (person.jobs) { // TV structure
        return isDepartment ?
          person.department === filter :
          person.jobs.some(job => filter.includes(job.job));
      }
      // Movie structure
      return isDepartment ?
        person.department === filter :
        filter.includes(person.job);
    })
    .sort((a, b) => (b.total_episode_count || 0) - (a.total_episode_count || 0))
    .slice(0, limit)
    .map(person => person.name);
}

/**
 * Extract cast members.
 * @param {Object} credits - The credits object from TMDB.
 * @param {number} limit - The maximum number of cast members to include.
 * @returns {Array<string>} - An array of cast member names.
 */
function extractCast(credits, limit = 6) {
  if (!credits || !credits.cast) return [];
  return credits.cast.slice(0, limit).map((actor) => actor.name);
}

/**
 * Show an inline search popup panel for selecting a movie from multiple search results.
 * The popup is embedded within the search input container and appears underneath the input.
 * @param {Array} results - Array of TMDB search results.
 * @param {string} title - The title to display.
 * @param {number|null} year - Optional release year.
 * @returns {Promise<Object|null>} - The selected movie object or null if cancelled.
 */
async function showSelectionPopup(results, title, year = null) {
  return new Promise((resolve) => {
    // Get the dedicated inline search popup element.
    const searchPopup = document.getElementById("search-popup");
    if (!searchPopup) {
      console.error("Search popup element not found!");
      resolve(null);
      return;
    }

    // Build the dynamic search string.
    const searchString = year ? `${title} (${year})` : title; // ### unused 

    // Populate the inline popup with dynamic content.
    searchPopup.innerHTML = `
      <!-- Display the search string as a header -->
      <!--div class="search-popup-header">${searchString}</div-->
      <div id="search-results-container" class="horizontal-scroll">
        ${results
        .map(
          (result, index) => `
            <div class="movie-option" data-index="${index}">
              ${result.poster_path
              ? `<img src="${tmdbImgBaseUrl}w154${result.poster_path}" alt="${result.title}" class="movie-poster" />`
              : `${createPlaceholderSVG(154, 231, 'poster')}`
            }
              <div class="movie-info">
                <div class="movie-title">
                  ${result.title} <span class="movie-year">(${result.release_date ? result.release_date.split("-")[0] : "Unknown"
            })</span>
                </div>
              </div>
            </div>
          `
        )
        .join("")}
      </div>
    `;

    // Show the inline search popup.
    searchPopup.classList.remove("hidden");

    // Get the container with the search results.
    const resultsContainer = document.getElementById("search-results-container");

    // Define a named click handler for result selection.
    function onSelect(event) {
      const movieOption = event.target.closest(".movie-option");
      if (movieOption) {
        const index = parseInt(movieOption.dataset.index, 10);
        cleanup();
        resolve(results[index]);
      }
    }
    resultsContainer.addEventListener("click", onSelect);

    // Define a named handler for cancellation when clicking outside the popup.
    function onDocumentClick(event) {
      if (!searchPopup.contains(event.target)) {
        cleanup();
        resolve(null);
      }
    }
    document.addEventListener("click", onDocumentClick);

    // Define a named handler for pressing the Escape key.
    function onKeydown(event) {
      if (event.key === "Escape") {
        cleanup();
        resolve(null);
      }
    }
    window.addEventListener("keydown", onKeydown);

    // Cleanup function to hide the popup and remove all event listeners.
    function cleanup() {
      searchPopup.classList.add("hidden");
      searchPopup.innerHTML = "";
      resultsContainer.removeEventListener("click", onSelect);
      document.removeEventListener("click", onDocumentClick);
      window.removeEventListener("keydown", onKeydown);
    }
  });
}

  async function fetchAndStoreSeasonEpisodes(context, totalEpisodes) {
  const { db, showId: seriesID, seasonNumber, imdbID } = context;
  const episodeQueries = [];
  const episodesToStore = [];
  const creditsMap = new Map();
  const jobEpisodeCounts = new Map();

  const seriesName = mediaCache.has(imdbID) 
  ? (mediaCache.get(imdbID).Title || mediaCache.get(imdbID).name || "No name found") 
  : "No name found";

  try {
    showNotification(`Fetching Episodes for "${seriesName}", Season ${seasonNumber}...`, true);
    
    // Create query objects for TMDb API - use correct URL format without leading slash
    for (let episodeNumber = 1; episodeNumber <= totalEpisodes; episodeNumber++) {
      episodeQueries.push({
        tmdbID: seriesID,
        endpoint: `season/${seasonNumber}/episode/${episodeNumber}?append_to_response=credits`
      });
    }
    
    // Query TMDB API with correct case
    const episodeResults = await queryTMDB(episodeQueries, "custom", "tv");
    
    episodeResults.forEach(episodeData => {
      if (!episodeData) return;
      
      // Store episode-specific information
      const transformedEpisode = {
        tmdbID: seriesID,
        season_number: episodeData.season_number,
        episode_number: episodeData.episode_number,
        air_date: episodeData.air_date,
        name: episodeData.name,
        overview: episodeData.overview,
        id: episodeData.id,
        production_code: episodeData.production_code,
        runtime: episodeData.runtime,
        still_path: episodeData.still_path,
        vote_average: episodeData.vote_average,
        vote_count: episodeData.vote_count
      };
      
      episodesToStore.push(transformedEpisode);
      
      const seasonEpisodeString = `s${episodeData.season_number.toString().padStart(2, '0')}e${episodeData.episode_number.toString().padStart(2, '0')}`;
      
      // Skip if no credits data
      if (!episodeData.credits) {
        console.warn(`No Credits for Episode ${seasonEpisodeString}`);
        return;
      }
      
      const processCredits = (creditArray, role) => {
        if (!creditArray || !Array.isArray(creditArray)) return;
        
        creditArray.forEach(person => {
          if (!person || !person.id) return;
          
          // Create a key that includes the original credit_id from TMDB
          const key = `${seriesID}-${person.id}-${person.credit_id}`;
          const isCrew = role === 'crew';
          
          // Track episode counts for crew jobs
          if (isCrew && person.job) {
            const jobKey = `${seriesID}-${person.id}-${person.job}`;
            if (!jobEpisodeCounts.has(jobKey)) {
              jobEpisodeCounts.set(jobKey, new Set());
            }
            jobEpisodeCounts.get(jobKey).add(seasonEpisodeString);
          }
          
          if (!creditsMap.has(key)) {
            // Create new credit object with original TMDB fields
            const creditObj = {
              tmdbID: seriesID,
              personID: person.id,
              credit_id: person.credit_id,  // Use the original TMDB credit_id
              role: role,
              name: person.name,
              profile_path: person.profile_path,
              gender: person.gender,
              known_for_department: person.known_for_department,
              original_name: person.original_name,
              popularity: person.popularity,
              adult: person.adult,
              season_episodes: [seasonEpisodeString]
            };
            
            // Add role-specific fields
            if (isCrew) {
              creditObj.department = person.department;
              creditObj.job = person.job;
              creditObj.episode_count = person.episode_count || 1;
            } else {
              creditObj.cast_id = person.cast_id;
              creditObj.character = person.character;
              creditObj.order = person.order;
              creditObj.episode_count = person.episode_count || 1;
            }
            
            creditsMap.set(key, creditObj);
          } else {
            // Add to existing season_episodes array if not already present
            const credit = creditsMap.get(key);
            if (!credit.season_episodes.includes(seasonEpisodeString)) {
              credit.season_episodes.push(seasonEpisodeString);
            }
          }
        });
      };
      
      // Process each type of credit with existence checks
      processCredits(episodeData.credits.cast, 'cast');
      processCredits(episodeData.credits.crew, 'crew');
      processCredits(episodeData.credits.guest_stars, 'guest_star');
    });
    
    // Update crew episode counts using values() to avoid unused key
    for (const creditData of creditsMap.values()) {
      if (creditData.role === 'crew' && creditData.job) {
        const jobKey = `${seriesID}-${creditData.personID}-${creditData.job}`;
        creditData.episode_count = jobEpisodeCounts.get(jobKey)?.size || creditData.episode_count;
      }
    }
    
    // Store episodes
    if (episodesToStore.length > 0) {
      await saveToDatabase(db, episodesToStore, 'series_episodes');
    }
    
    // Improved promise handling for storing credits
    let storedCreditsCount = 0;
    if (creditsMap.size > 0) {
      await new Promise((resolve, reject) => {
        const transaction = db.transaction("series_credits", "readwrite");
        let pendingRequests = 0;
        
        transaction.oncomplete = () => resolve();
        transaction.onerror = (e) => reject(e.target.error);
        
        const store = transaction.objectStore("series_credits");
        
        for (const creditData of creditsMap.values()) {
          // Sort episodes and remove duplicates
          creditData.season_episodes = [...new Set(creditData.season_episodes)].sort();
          
          pendingRequests++;
          const getRequest = store.get([seriesID, creditData.personID, creditData.credit_id]);
          
          getRequest.onsuccess = (event) => {
            const existing = event.target.result;
            
            if (existing) {
              // Merge and deduplicate existing episodes
              const merged = [...new Set([
                ...(existing.season_episodes || []),
                ...creditData.season_episodes
              ])].sort();
              
              store.put({
                ...existing,
                season_episodes: merged
              });
            } else {
              store.put(creditData);
            }
            
            storedCreditsCount++;
            pendingRequests--;
            if (pendingRequests === 0) {
              // All requests processed, but transaction may still be committing
              // resolve happens via transaction.oncomplete
            }
          };
          
          getRequest.onerror = (e) => {
            console.error(`Error storing Credit for ${creditData.name}:`, e.target.error);
            pendingRequests--;
            if (pendingRequests === 0) {
              // Continue even if some credits had errors
            }
          };
        }
        
        // Handle edge case with no requests
        if (pendingRequests === 0) {
          resolve();
        }
      });
    }
    
    // Include series name and counts in final notification
    showNotification(
      `Successfully loaded ${episodesToStore.length} Episodes and ${storedCreditsCount} Credits for "${seriesName}" Season ${seasonNumber}.`,
      false
    );
    
    return {
      episodesCount: episodesToStore.length,
      creditsCount: creditsMap.size
    };
  } catch (error) {
    console.error(`Error fetching Episodes for Series ${seriesID}, Season ${seasonNumber}:`, error);
    showNotification(`Error loading Episodes for "${seriesName}" Season ${seasonNumber}: ${error.message}`, false);
    throw error;
  }
}

async function queryAPI(items, getUrl, getApiKey, throttleLimit, throttleInterval, processResult, errorHandler, isBackgroundProcess = false) {
  const itemsToQuery = Array.isArray(items) ? items : [items];
  const totalItems = itemsToQuery.length;
  let successCount = 0, skippedCount = 0, errorCount = 0;

  for (let i = 0; i < totalItems; i++) {
    const item = itemsToQuery[i];
    let url = null; // Define url outside try block to avoid scoping issues

    try {
      // Throttle requests if needed
      if (throttleLimit > 0 && i > 0 && i % throttleLimit === 0) {
        await new Promise(resolve => setTimeout(resolve, throttleInterval));
      }

      // Get API key
      const apiKey = await getApiKey();
      if (!apiKey) throw new Error("API keys exhausted");

      // Build URL
      url = getUrl(item, apiKey);
      if (!url) {
        skippedCount++;
        continue;
      }

      // Fetch data
      const response = await fetch(url);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const data = await response.json();
      if (data.Response === "False") throw new Error(data.Error || "API error");

      // Process result
      processResult?.(data);
      successCount++;

      // If this is a background process, update the processed count in backgroundPeopleQueue
      if (isBackgroundProcess && backgroundPeopleQueue._updateProgress) {
        backgroundPeopleQueue._updateProgress(successCount); // Pass the total success count
      }
    } catch (error) {
      // Custom error handling
      if (errorHandler) {
        try {
          const handledResult = await errorHandler(error, item, url);
          if (handledResult !== undefined) {
            processResult?.(handledResult);
            successCount++;
            continue;
          }
        } catch (handlerError) {
          console.error("Error in error handler:", handlerError);
        }
      }

      console.error(`Error querying API:`, error);
      errorCount++;
    }
  }

  console.log(`Results: ${successCount} success, ${skippedCount} skipped, ${errorCount} errors`);
  return { successCount, skippedCount, errorCount };
}

function tmdbErrorHandler(error, _item, url) {
  // Check for 404 on episode endpoints
  if (error.status === 404 && url.includes('/episode/')) {
    const matches = url.match(/tv\/(\d+)\/season\/(\d+)\/episode\/(\d+)/);
    if (matches) {
      console.log("Handled 404 for episode:", url);
      return {
        id: -1,
        season_number: parseInt(matches[2]),
        episode_number: parseInt(matches[3]),
        name: `Episode ${matches[3]}`,
        overview: "No information available",
        air_date: null,
        still_path: null,
        vote_average: 0
      };
    }
  }
  return undefined; // Let other errors propagate
}

async function queryTMDB(data, endpoint, tmdbCategory, isBackgroundProcess = false) {
  const results = [];

  await queryAPI(
    data,
    (item, apiKey) => {
      const { tmdbID, title, year } = item;

      // Unified URL builder with validation
      switch (endpoint) {
        case "search": {
          if (!title?.trim() || !year) {
            console.warn("Skipping search query - missing title/year:", item);
            return null;
          }
          return `${tmdbApiBaseUrl}search/${tmdbCategory}?api_key=${apiKey}&query=${encodeURIComponent(title)}&year=${year}`;
        }

        case "details": {
          if (!tmdbID) {
            console.warn("Skipping details query - missing tmdbID:", item);
            return null;
          }
          return `${tmdbApiBaseUrl}${tmdbCategory}/${tmdbID}?api_key=${apiKey}`;
        }

        case "custom": {
          if (!tmdbID || !item.endpoint) {
            console.warn("Skipping custom query - missing tmdbID/endpoint:", item);
            return null;
          }
          const separator = item.endpoint.includes("?") ? "&" : "?";
          return `${tmdbApiBaseUrl}${tmdbCategory}/${tmdbID}/${item.endpoint}${separator}api_key=${apiKey}`;
        }

        default: {
          console.warn("Unknown TMDB endpoint:", endpoint);
          return null;
        }
      }
    },
    () => apiKeyManager.getTmdbKey(),
    tmdbThrottleLimit,
    tmdbThrottleInterval,
    (result) => result !== undefined && results.push(result), // Filter here
    tmdbErrorHandler,
    isBackgroundProcess
  );

  return results;
}

// Simple TMDB query helper function
async function queryTMDBSimple(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP error: ${response.status}`);
  return await response.json();
}

// Function to query OMDB for a single ID
async function queryOMDB(imdbID, retryCount = 0) {
  const apiKey = await apiKeyManager.getNextOmdbKey();
  if (!apiKey) {
    showNotification("All OMDb API keys exhausted", false);
    throw new Error("All OMDb API keys exhausted");
  }

  const url = `${omdbApiBaseUrl}?apikey=${apiKey}&i=${imdbID}`;

  try {
    const response = await fetch(url);

    // If unauthorized, mark key as invalid and retry with another key
    if (response.status === 401) {
      console.warn(`API key is invalid or expired`);
      // Mark this key as reached limit to avoid using it again
      setApiCallCount(apiKey, omdbRateLimit);

      // Try again with a different key (but limit retries)
      if (retryCount < apiKeyManager.getOmdbKeys().length - 1) {
        return queryOMDB(imdbID, retryCount + 1);
      } else {
        throw new Error("All OMDb API keys are invalid or expired");
      }
    }

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();

    // Successfully used the API key - update its count
    updateApiCallCount(apiKey);

    // Filter out N/A values from OMDB response
    return Object.fromEntries(
      Object.entries(data).filter(([, v]) => v !== "N/A")
    );
  } catch (error) {
    // If it's not already a 401 error we handled above, rethrow it
    if (!error.message.includes("All OMDb API keys")) {
      throw error;
    }
  }
}

/**
 * Helper for query functions
 * @param {*} db 
 * @param {*} storeName 
 * @param {*} imdbID 
 * @returns 
 */
async function getFromDatabase(db, storeName, imdbID) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([storeName], 'readonly');
    const store = transaction.objectStore(storeName);
    const request = store.get(imdbID);

    request.onsuccess = () => resolve(request.result || null);
    request.onerror = () => reject(request.error);
  });
}

/**
 * Get the height of the menu bar, including any necessary buffer space.
 * @returns {number} - The total height of the menu bar plus buffer in pixels.
 */
function getMenuBarHeight() {
  const menuBar = document.querySelector(".menu-container");
  const MENU_PADDING = 10; 

  if (!menuBar) return 50 + MENU_PADDING; // Fallback 

  try {
    const style = window.getComputedStyle(menuBar);
    const marginBottom = parseInt(style.marginBottom) || 0;

    // Include the buffer as part of the menu height calculation
    return menuBar.offsetHeight + marginBottom + MENU_PADDING;
  } catch (error) {
    console.warn("Error calculating menu height:", error);
    return 50 + MENU_PADDING; // Fallback 
  }
}

/**
 * Show or update a notification popup with a CSS spinner.
 * @param {string} message - The message to display.
 * @param {boolean} isProgress - Whether this is a progress update or a final message.
 * @param {boolean} isBackgroundTask - Whether this is a background task notification.
 * @param {string} id - Optional ID for the notification. If not provided, a unique ID will be generated.
 * @returns {string} The ID of the notification.
 */
function showNotification(message, isProgress, isBackgroundTask = false) {
  // First, look for any existing progress notifications that should be cleared
  // when a non-progress notification is shown
  if (!isProgress && !isBackgroundTask) {
    // Find and remove any non-background progress notifications
    const progressIndex = notifications.findIndex(n => 
      n.isProgress && !n.isBackgroundTask
    );
    
    if (progressIndex !== -1) {
      notifications.splice(progressIndex, 1);
    }
  }
  
  // Find existing notification of same type
  const existingNotification = notifications.find(n => n.isBackgroundTask === isBackgroundTask);
  
  if (existingNotification) {
    // Update existing notification
    existingNotification.message = message;
    existingNotification.isProgress = isProgress;
  } else {
    // Add new notification
    notifications.push({ message, isProgress, isBackgroundTask });
  }
  
  // Display highest-priority notification
  const activeNotification = notifications.find(n => !n.isBackgroundTask) || 
                           notifications.find(n => n.isBackgroundTask);
  
  // Update UI
  const notificationContainer = document.querySelector('.query-notification');
  const spinnerContainer = notificationContainer.querySelector('.progress-spinner');
  const progressText = notificationContainer.querySelector('.progress-text');
  
  spinnerContainer.style.display = activeNotification.isProgress ? 'block' : 'none';
  progressText.textContent = activeNotification.message;
  notificationContainer.classList.remove('hidden');
  notificationContainer.style.opacity = 1;
  
  // Auto-hide logic for non-progress notifications
  if (!activeNotification.isBackgroundTask && !activeNotification.isProgress) {
    setTimeout(() => removeNotification(activeNotification), 3000);
  }
}

/**
 * Remove a notification from the queue.
 * @param {string|function|object} target - Either the notification ID, a function that determines 
 *                               which notification to remove, or the notification object itself.
 */
function removeNotification(target) {
  // Handle different target types
  let index = -1;
  
  if (typeof target === 'string') {
    // Target is an ID
    index = notifications.findIndex(n => n.id === target);
  } else if (typeof target === 'function') {
    // Target is a finder function
    index = notifications.findIndex(target);
  } else if (target && typeof target === 'object') {
    // Target is a notification object reference
    index = notifications.indexOf(target);
  }
  
  if (index !== -1) {
    notifications.splice(index, 1);
    
    // UI cleanup logic
    if (notifications.length === 0) {
      document.querySelector('.query-notification').classList.add('hidden');
    } else if (index === 0) {
      // Update display if top notification was removed
      const nextNote = notifications[0];
      const spinner = document.querySelector('.progress-spinner');
      const text = document.querySelector('.progress-text');
      
      spinner.style.display = nextNote.isProgress ? 'block' : 'none';
      text.textContent = nextNote.message;
    }
  }
}

function initTooltips() {
  // Create a single IntersectionObserver for lazy loading tooltips
  const tooltipObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const mediaCard = entry.target;
        if (mediaCard.dataset.needsTooltip === "true") {
          createTooltipForCard(mediaCard);
        }
        tooltipObserver.unobserve(mediaCard);
      }
    });
  }, {
    rootMargin: '200px', // Load tooltips when cards are within 200px of viewport
    threshold: 0.1
  });

  // Start observing cards that need tooltips
  document.querySelectorAll('.media-card[data-needs-tooltip="true"]').forEach(card => {
    tooltipObserver.observe(card);
  });

  // Add mouseover handler for cards that aren't visible yet
  document.querySelector('.card-grid').addEventListener('mouseover', function (event) {
    const mediaCard = event.target.closest('.media-card[data-needs-tooltip="true"]');
    if (mediaCard) {
      createTooltipForCard(mediaCard);
    }
  }, { passive: true });

  return tooltipObserver;
}

function createTooltipForCard(mediaCard) {
  const imdbID = mediaCard.dataset.imdbid;
  const media = mediaCache.get(imdbID);

  if (media && !mediaCard.querySelector('.tooltip-text')) {
    // Create tooltip
    const tooltip = document.createElement("div");
    tooltip.className = "tooltip-text";
    tooltip.innerHTML = generateTooltip(media);
    mediaCard.appendChild(tooltip);

    // Add icons
    const topRowIcons = createTopRowIcons(imdbID);
    if (topRowIcons) {
      mediaCard.appendChild(topRowIcons);
    }

    // Add visibility event listeners
    mediaCard.addEventListener('mouseenter', () => {
      mediaCard.classList.add('tooltip-visible');
    });

    mediaCard.addEventListener('mouseleave', () => {
      mediaCard.classList.remove('tooltip-visible');
    });

    // Mark as processed
    delete mediaCard.dataset.needsTooltip;
  }
}

/**
 * UI: Custom confirm popup
 * @param {*} message 
 * @param {*} onConfirm 
 * @returns 
 */
function showCustomConfirm(message, onConfirm) {
  return new Promise((resolve) => {
    // Create content for the confirmation popup
    const confirmContent = `
      <div class="import-dialog">
        <h3 class="warning">⚠ Confirmation ⚠</h3>
        <div class="custom-text">${message}</div>
        <div class="button-group">
          <button id="confirm-delete" class="popup-button delete">Delete</button>
          <button id="confirm-cancel" class="popup-button right">Cancel</button>
        </div>
      </div>
    `;
    
    // Use the showPopup function with custom event listeners
    showPopup('confirm', confirmContent, {
      listeners: {
        '#confirm-cancel': {
          click: () => {
            closePopup('confirm');
            resolve(false);
            // console.log("User canceled the action.");
          }
        },
        '#confirm-delete': {
          click: () => {
            closePopup('confirm');
            if (typeof onConfirm === 'function') {
              onConfirm();
            }
            resolve(true);
          }
        }
      }
    });
  });
}

/**
 * Save one or multiple updated movies, series or endpoint data back to IndexedDB.
 * Works for both OMDB and TMDB data, dynamically handling object stores and merging logic.
 * @param {Object} db - The already open IndexedDB instance.
 * @param {Object|Array} data - A single media object, an array of objects, or endpoint-specific data.
 * @param {string} [storeName="movies"] - The name of the object store (e.g., "movies", "release_dates", "movie_keywords").
 */
async function saveToDatabase(db, data, storeName = "movies") {
  try {
    const transaction = db.transaction([storeName], "readwrite");
    const store = transaction.objectStore(storeName);

    // Handle both single and batch saves
    let items = Array.isArray(data) ? data : [data];

    // Add a timestamp field ONLY to "movies" store
    if (storeName === "movies" || storeName === "series") {
      const currentTimestamp = Date.now();
      items = items.map((item) => ({
        ...item,
        timestamp: currentTimestamp,
      }));
    }

    for (const item of items) {
      if ("tmdbID" in item) {
        item.tmdbID = Number(item.tmdbID);
      }

      // Save the record in IndexedDB
      const usesInlineKeys = store.keyPath !== null;

      if (usesInlineKeys) {
        // Special handling for "_credits"
        if (storeName.endsWith('_credits')) {
          // Compute the default (temporary) credit_id as it was assigned during migration.
          const computedCreditID = `${item.tmdbID}-${item.personID}`;
          // If the incoming record has a credit_id different than the computed one,
          // it means it's the new value from TMDB.
          if (item.credit_id !== computedCreditID) {
            // Try to retrieve an existing record that still uses the temporary credit_id.
            try {
              const oldRecord = await new Promise((resolve, reject) => {
                const getReq = store.get([item.tmdbID, item.personID, computedCreditID]);
                getReq.onsuccess = (event) => resolve(event.target.result);
                getReq.onerror = (event) => reject(event.target.error);
              });
              if (oldRecord) {
                // If found, delete the old temporary record.
                await new Promise((resolve, reject) => {
                  const delReq = store.delete([item.tmdbID, item.personID, computedCreditID]);
                  delReq.onsuccess = resolve;
                  delReq.onerror = (event) => reject(event.target.error);
                });
                console.log(
                  `Deleted old temporary record for ${storeName}: [${item.tmdbID}, ${item.personID}, ${computedCreditID}]`
                );
              }
            } catch (error) {
              console.error(`Error handling temporary ${storeName}: record:`, error);
            }
          }
        }

        // For stores with inline keys (e.g., composite keys like ["tmdbID", "keywordID"])
        try {
          await store.put(item); // Let IndexedDB evaluate the key path
        } catch (error) {
          console.error(`Error saving to ${storeName} with item:`, item, error);
          throw error; // Re-throw to be caught by the outer try/catch
        }
      } else {
        // For stores without inline keys, manually provide the primary key
        let primaryKey;
        if (storeName === "movies" || storeName === "series") {
          primaryKey = item.imdbID; // Use imdbID for movies and series
        } else {
          // No other stores should reach this code path now
          console.warn(`Store '${storeName}' has no special key handling but doesn't use inline keys.`);
          continue;
        }

        if (!primaryKey) {
          console.warn(`Skipping entry: Missing primary key for store '${storeName}'.`, item);
          continue;
        }

        await store.put(item, primaryKey); // Save using explicit keys
      }
    }

    await transaction.complete; // Ensure the transaction completes successfully
    return true;
  } catch (error) {
    console.error(`Failed to save data to '${storeName}' store:`, error);
    throw error; // Re-throw to allow caller to handle the error
  }
}

/**
 * DB: merge/update existing with new data
 * @param {*} existingData 
 * @param {*} newData 
 * @param {*} logUpdates 
 * @returns 
 */
function mergeEndpointData(existingData, newData, logUpdates = false) {
  if (!existingData) return newData;

  const mergedData = { ...existingData };

  // Enhanced ID conflict resolution
  if (newData.id && !newData.tmdbID && !mergedData.tmdbID) {
    newData.tmdbID = newData.id;
    delete newData.id;
  }

  for (const [key, value] of Object.entries(newData)) {
    if (isRelationalField(value)) {
      if (logUpdates) console.log(`Skipping relational field "${key}"`);
      continue;
    }

    // Handle arrays first
    if (Array.isArray(value)) {
      // If existing value is not an array, or if it is an array that's empty or filled with undefined, just replace it.
      if (!Array.isArray(mergedData[key]) ||
        mergedData[key].length === 0 ||
        mergedData[key].every(item => item === undefined)) {
        mergedData[key] = value;
        continue;
      }

      // Improved deduplication for arrays that have valid elements.
      const getUniqueKey = (item) => {
        if (item === undefined || item === null) return '';
        if (typeof item.id !== "undefined" && item.id !== null) {
          return item.id.toString();
        }
        if (typeof item.tmdbID !== "undefined" && item.tmdbID !== null) {
          return item.tmdbID.toString();
        }
        return JSON.stringify(item);
      };

      const existingKeys = new Set(
        mergedData[key]
          .filter(item => item !== undefined && item !== null)
          .map(getUniqueKey)
      );
      const newItems = value.filter(
        item => item !== undefined && item !== null && !existingKeys.has(getUniqueKey(item))
      );

      if (newItems.length > 0) {
        mergedData[key] = [...mergedData[key], ...newItems];
        if (logUpdates) console.log(`Added ${newItems.length} items to ${key}`);
      }

      // Fix Country and Language fields if necessary
      if (key === "Country") {
        const allowedCountry = "United States";
        const erroneousVariants = new Set(["usa", "us", "united states of america"]);
        mergedData[key] = mergedData[key].map(country =>
          erroneousVariants.has(country.toLowerCase()) ? allowedCountry : country
        );
        mergedData[key] = [...new Set(mergedData[key])]; // Remove duplicates
      } else if (key === "Language") {
        mergedData[key] = mergedData[key].filter(lang =>
          typeof lang === "string" && lang.toLowerCase() !== "n/a" && lang.length !== 2
        );
      }
      continue;
    }

    // Handle nested objects
    if (typeof value === "object" && value !== null) {
      mergedData[key] = mergeEndpointData(
        mergedData[key] || {},
        value,
        logUpdates
      );
      continue;
    }

    // Primitive values
    if (mergedData[key] !== value) {
      mergedData[key] = value;
      if (logUpdates) console.log(`Updated ${key} to`, value);
    }
  }

  return mergedData;
}

/**
 * Check if a field is relational based on its structure.
 * @param {*} value - The field value to check.
 * @returns {boolean} - True if the field is relational, false otherwise.
 */
function isRelationalField(value) {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every(item =>
      item &&
      typeof item === "object" &&
      (item.id || item.tmdbID) // ONLY require ID presence
    )
  );
}

/**
 * Open IndexedDB
 * @returns {Promise<IDBDatabase>} - A promise that resolves to the opened database instance.
 */
async function openDB() {
  return new Promise((resolve, reject) => {
    // console.log(`Attempting to open DB "${DB_NAME}" version ${DB_VERSION}`);
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = (event) => {
      const dbInstance = event.target.result;
      // The transaction is automatically created for version changes
      const transaction = event.target.transaction;
      const oldVersion = event.oldVersion; // Get the previous version

      console.log(`Upgrading IndexedDB from version ${oldVersion} to ${DB_VERSION}...`);

      // Setup error handling for the upgrade transaction itself
      transaction.onerror = (event) => {
        console.error(`Transaction error during DB upgrade:`, event.target.error);
        reject(new Error(`DB upgrade transaction failed: ${event.target.error}`));
      };
      transaction.onabort = (event) => {
        console.warn(`Transaction aborted during DB upgrade:`, event.target.error);
        reject(new Error(`DB upgrade transaction aborted: ${event.target.error}`));
      };
      transaction.oncomplete = () => {
        console.log("DB schema upgrade transaction completed.");
        // Note: Seeding happens within this transaction before it completes.
      };


      try {
        // Perform all schema updates & data seeding within this transaction
        initializeDB(dbInstance, transaction, oldVersion);
        console.log("initializeDB called successfully during upgrade.");
      } catch (error) {
        console.error(`Error during onupgradeneeded schema/seed execution:\n${error.message}`, error.stack);
        // Attempt to abort the transaction explicitly on synchronous error
        try { transaction.abort(); } catch (e) { console.error("Failed to abort transaction:", e); }
        reject(error); // Reject the openDB promise
      }
    };

    request.onsuccess = (event) => {
      const dbInstance = event.target.result;
      console.log(`IndexedDB "${DB_NAME}" version ${dbInstance.version} opened successfully.`);
      // Assign the db instance globally or pass it around as needed
      db = dbInstance;
      resolve(dbInstance);
    };

    request.onerror = (event) => {
      console.error(`Error opening IndexedDB "${DB_NAME}":`, event.target.error);
      reject(new Error(`Error opening IndexedDB: ${event.target.error?.message || event.target.errorCode}`));
    };
  });
}

/**
 * Initialize or upgrade IndexedDB schema and seed initial data.
 * This function runs within the versionchange transaction provided by onupgradeneeded.
 * 
 * @param {IDBDatabase} dbInstance - The database instance from event.target.result
 * @param {IDBTransaction} transaction - The active version change transaction
 * @param {number} oldVersion - The previous version number of the database
 */
function initializeDB(dbInstance, transaction, oldVersion) {
  console.log(`Initializing database: upgrading from v${oldVersion} to v${DB_VERSION}`);
  
  // --- Track operation promises ---
  const operations = [];
  
  // --- Schema Definition ---
  // Unified list of all object stores to create or update
  const storesToCreate = [
    // Movies Store
    {
      name: "movies",
      options: { keyPath: "imdbID" },
      indexes: [
        { name: "dataTitle", keyPath: "dataTitle", unique: false },
        { name: "tmdbID", keyPath: "tmdbID", unique: true },
        { name: "Year", keyPath: "Year", unique: false },
        { name: "Genre", keyPath: "Genre", multiEntry: true, unique: false },
        { name: "popularity", keyPath: "popularity", unique: false },
        { name: "timestamp", keyPath: "timestamp", unique: false }
      ],
      // obsoleteIndexes: ["indexname1", "indexname2"], // Remove obsolete index from store
    },

    { name: "movie_details", options: { keyPath: "tmdbID" } },

    {
      name: "movie_credits",
      options: { keyPath: ["tmdbID", "personID", "credit_id"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false }, 
        { name: "personID", keyPath: "personID", unique: false }, 
        { name: "role", keyPath: "role", unique: false }, 
      ],
    },

    {
      name: "movie_keywords",
      options: { keyPath: ["tmdbID", "keywordID"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false },
        { name: "keywordID", keyPath: "keywordID", unique: false },
      ],
    },

    {
      name: "movie_genres",
      options: { keyPath: ["tmdbID", "genreID"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false },
        { name: "genreID", keyPath: "genreID", unique: false },
      ],
    },

    // Series Store
    {
      name: "series",
      options: { keyPath: "imdbID" },
      indexes: [
        { name: "dataTitle", keyPath: "dataTitle", unique: false },
        { name: "tmdbID", keyPath: "tmdbID", unique: true },
        { name: "Year", keyPath: "Year", unique: false },
        { name: "Genre", keyPath: "Genre", multiEntry: true, unique: false },
        { name: "popularity", keyPath: "popularity", unique: false },
        { name: "timestamp", keyPath: "timestamp", unique: false } 
      ],
    },

    { name: "series_details", options: { keyPath: "tmdbID" } },

    {
      name: "series_episodes",
      options: { keyPath: ["tmdbID", "season_number", "episode_number"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false },
        { name: "season", keyPath: ["tmdbID", "season_number"], unique: false } 
      ],
    },

    {
      name: "series_credits",
      options: { keyPath: ["tmdbID", "personID", "credit_id"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false },
        { name: "personID", keyPath: "personID", unique: false },
        { name: "role", keyPath: "role", unique: false },
        { name: "season_episodes", keyPath: "season_episodes", multiEntry: true }
      ],
    },

    {
      name: "series_keywords",
      options: { keyPath: ["tmdbID", "keywordID"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false },
        { name: "keywordID", keyPath: "keywordID", unique: false },
      ],
    },

    {
      name: "series_genres",
      options: { keyPath: ["tmdbID", "genreID"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false },
        { name: "genreID", keyPath: "genreID", unique: false }
      ],
    },

    {
      name: "ratings",
      options: { keyPath: ["imdbID", "Source"] },
      indexes: [
        { name: "imdbID", keyPath: "imdbID", unique: false } 
      ],
    },

    {
      name: "people",
      options: { keyPath: "personID" },
      indexes: [
        { name: "name", keyPath: "name", unique: false }
      ],
    },

    {
      name: "release_dates",
      options: { keyPath: ["tmdbID", "countryCode", "release_date"] },
      indexes: [
        { name: "tmdbID", keyPath: "tmdbID", unique: false }
      ],
    },

    { name: "keywords", options: { keyPath: "keywordID" } },
    { name: "genres", options: { keyPath: "genreID" } },

    {
      name: "lists",
      options: { keyPath: ["username", "imdbID", "type", "list"] },
      indexes: [
        { name: "username", keyPath: "username", unique: false },
        { name: "imdbID", keyPath: "imdbID", unique: false },
        { name: "type", keyPath: "type", unique: false },
        { name: "list", keyPath: "list", unique: false },
        { name: "dateAdded", keyPath: "dateAdded", unique: false },
        { name: "username_list", keyPath: ["username", "list"], unique: false }
      ],
    },
    
    {
      name: "lists_metadata",
      options: { keyPath: ["username", "type", "list"] },
      indexes: [
        { name: "username", keyPath: "username", unique: false },
        { name: "type", keyPath: "type", unique: false },
        { name: "username_type", keyPath: ["username", "type"], unique: false },
        { name: "list", keyPath: "list", unique: false }, 
        { name: "username_list", keyPath: ["username", "list"], unique: true },
        { name: "created", keyPath: "created", unique: false },
        { name: "lastUpdated", keyPath: "lastUpdated", unique: false },
        { name: "avgRating", keyPath: "avgRating", unique: false }, // For sorting by rating
        { name: "ratingsCount", keyPath: "ratingsCount", unique: false } // For popularity metrics
      ],
    },
    {
      name: "links", options: { keyPath: ["imdbID", "type", "url"] },
      indexes: [
        { name: "imdbID", keyPath: "imdbID", unique: false },
      ],
    },

    {
      name: "provider_data",
      options: { keyPath: "id" } 
    },

    {
      name: "user_settings",
      options: { keyPath: "username" },
    },

    {
      name: "media_settings", 
      options: { keyPath: "id", autoIncrement: true },
      indexes: [
        { name: "imdbID", keyPath: "imdbID", unique: false },
        { name: "username", keyPath: "username", unique: false },
        { name: "username_imdbID_type", keyPath: ["username", "imdbID", "type"], unique: true },
        { name: "type", keyPath: "type", unique: false }
      ],
    },

    {
      name: "notes",
      options: { keyPath: "id", autoIncrement: true },
      indexes: [
        { name: "imdbID", keyPath: "imdbID", unique: false },
        { name: "username_imdbID", keyPath: ["username", "imdbID"], unique: false }, 
        { name: "username_imdbID_context", keyPath: ["username", "imdbID", "context"], unique: true }, 
        { name: "timestamp", keyPath: "timestamp", unique: false }
      ],
    },
  ];

  // --- Helper Functions ---

  /**
   * Create or update a store with proper promise handling
   */
  function createOrUpdateStore(storeName, options, indexes = [], obsoleteIndexes = []) {
    return new Promise((resolve, reject) => {
      let store;
      
      try {
        // Check if store exists
        if (!dbInstance.objectStoreNames.contains(storeName)) {
          store = dbInstance.createObjectStore(storeName, options);
          console.log(`Created ${storeName} object store.`);
        } else {
          store = transaction.objectStore(storeName);
          
          // Check if keyPath has changed
          if (options && options.keyPath && JSON.stringify(store.keyPath) != JSON.stringify(options.keyPath)) {
            console.warn(`KeyPath for ${storeName} changed from ${store.keyPath} to ${options.keyPath}.`);
            
            // Use a promise to handle the async count operation
            const countPromise = new Promise((resolveCount) => {
              const countRequest = store.count();
              countRequest.onsuccess = () => {
                const isEmpty = countRequest.result === 0;
                
                if (isEmpty) {
                  // Store is empty, safe to delete and recreate
                  dbInstance.deleteObjectStore(storeName);
                  store = dbInstance.createObjectStore(storeName, options);
                  console.log(`Recreated ${storeName} with new keyPath ${options.keyPath}.`);
                } else {
                  console.warn(`Store ${storeName} contains ${countRequest.result} records. Cannot change keyPath without data migration.`);
                }
                resolveCount();
              };
              
              countRequest.onerror = (event) => {
                console.error(`Error checking if store ${storeName} is empty:`, event.target.error);
                console.warn(`Cannot safely check contents of ${storeName}. Aborting keyPath change to prevent data loss.`);
                resolveCount(); // Continue despite error
              };
            });
            
            // Wait for count operation to complete before proceeding
            return countPromise.then(() => {
              // Now handle indexes after keyPath check is complete
              return manageIndexes(store, indexes || [], obsoleteIndexes || [], storeName)
                .then(resolve)
                .catch(reject);
            });
          } else {
            console.log(`Store ${storeName} already exists with matching keyPath.`);
            // Handle indexes directly
            return manageIndexes(store, indexes || [], obsoleteIndexes || [], storeName)
              .then(resolve)
              .catch(reject);
          }
        }
      } catch (error) {
        console.error(`Error creating/updating store ${storeName}:`, error);
        reject(new Error(`Failed to create/update store ${storeName}: ${error.message}`));
        return;
      }
      
      // If we created a new store, handle its indexes
      if (store && (!options || !options.keyPath)) {
        manageIndexes(store, indexes || [], obsoleteIndexes || [], storeName)
          .then(resolve)
          .catch(reject);
      } else if (store) {
        // Ensure indexes are managed even for the base case
        manageIndexes(store, indexes || [], obsoleteIndexes || [], storeName)
          .then(resolve)
          .catch(reject);
      }
    });
  }

  /**
   * Manage indexes with proper promise handling
   */
  function manageIndexes(store, indexes, obsoleteIndexes, storeName) {
    return new Promise((resolve) => {
      try {
        const existingIndexes = Array.from(store.indexNames);
        
        // Safely handle obsolete indexes even if undefined or not an array
        if (obsoleteIndexes && Array.isArray(obsoleteIndexes)) {
          obsoleteIndexes.forEach(indexName => {
            if (existingIndexes.includes(indexName)) {
              try {
                store.deleteIndex(indexName);
                console.log(`Deleted obsolete index ${indexName} from ${storeName}.`);
              } catch (error) {
                console.error(`Error deleting index ${indexName} from ${storeName}:`, error.message);
              }
            }
          });
        }
        
        // Safely handle indexes even if undefined or not an array
        if (indexes && Array.isArray(indexes)) {
          indexes.forEach(({ name, keyPath, unique = false, multiEntry = false }) => {
            if (!existingIndexes.includes(name)) {
              try {
                store.createIndex(name, keyPath, { unique, multiEntry });
                console.log(`Added index ${name} to ${storeName}.`);
              } catch (error) {
                console.error(`Error adding index ${name} to ${storeName}:`, error.message);
              }
            } else {
              // Check if index definition changed
              const existingIndex = store.index(name);
              if (
                JSON.stringify(existingIndex.keyPath) != JSON.stringify(keyPath) ||
                existingIndex.unique != unique ||
                existingIndex.multiEntry != multiEntry
              ) {
                console.warn(`Index ${name} on store ${storeName} definition changed. Recreating.`);
                
                try {
                  // Recreate index with new definition
                  store.deleteIndex(name);
                  store.createIndex(name, keyPath, { unique, multiEntry });
                  console.log(`Recreated index ${name} on ${storeName} with new definition.`);
                } catch (error) {
                  console.error(`Error recreating index ${name} on ${storeName}:`, error.message);
                }
              }
            }
          });
        }
        
        resolve();
      } catch (error) {
        console.error(`Error managing indexes for ${storeName}:`, error);
        resolve(); // Continue despite errors
      }
    });
  }

  /**
   * Delete stores not defined in the current schema
   */
  function deleteObsoleteStores() {
    return new Promise((resolve) => {
      try {
        const currentStoreNames = storesToCreate.map(store => store.name);
        
        Array.from(dbInstance.objectStoreNames).forEach(storeName => {
          if (!currentStoreNames.includes(storeName)) {
            try {
              dbInstance.deleteObjectStore(storeName);
              console.log(`Deleted obsolete store ${storeName}.`);
            } catch (error) {
              console.error(`Error deleting obsolete store ${storeName}:`, error.message);
            }
          }
        });
        
        resolve();
      } catch (error) {
        console.error("Error deleting obsolete stores:", error);
        resolve(); // Continue despite errors
      }
    });
  }
    
  /**
   * Seed default list metadata and other initial data
   */
  function seedInitialData() {
    return new Promise((resolve) => {
      if (oldVersion >= DB_VERSION) {
        console.log(`Skipping data seeding as oldVersion ${oldVersion} is not less than current DB_VERSION ${DB_VERSION}.`);
        resolve();
        return;
      }
      
      console.log(`Seeding default list metadata for DB version ${DB_VERSION}...`);
      
      try {
        // Data seeding lists metadata
        if (dbInstance.objectStoreNames.contains('lists_metadata')) {
          const metadataStore = transaction.objectStore('lists_metadata');
          
          // Default username
          const defaultUsername = 'default';
          const now = Date.now();
          
          // Default lists data
          const defaultListsData = [
            { list: 'watchlist', symbolId: 'watchlist', label: 'Watchlist', type: 'core', description: 'Movies and shows to watch' },
            { list: 'collection', symbolId: 'collection', label: 'Collection', type: 'core', description: 'Movies and shows you own' },
            { list: 'favourites', symbolId: 'favourites', label: 'Favourites', type: 'core', description: 'Your favorite movies and shows' },
            { list: 'watched', symbolId: 'eye', label: 'Watched', type: 'core', description: 'Movies and shows you have seen' },
          ];

          // Process each list
          defaultListsData.forEach(listData => {
            const metadataRecord = {
              username: defaultUsername,
              type: listData.type,
              list: listData.list,
              symbolId: listData.symbolId,
              label: listData.label,
              description: listData.description,
              created: now,
              lastUpdated: now,
              isPublic: true,
              avgRating: 0,
              ratingsCount: 0,
              ratings: {},
              tags: [listData.type],
              coverImageID: null
            };

            // Use put to overwrite existing records
            const putRequest = metadataStore.put(metadataRecord);
            
            putRequest.onerror = (event) => {
              console.error(`Error seeding default list ${listData.list}:`, event.target.error);
            };
          });
          
          console.log("Default list metadata seeding completed");
        }
        
        resolve();
      } catch (error) {
        console.error("Error during data seeding:", error);
        resolve(); // Continue despite errors
      }
    });
  }
  
  // --- Schema Creation / Update Execution ---
  
  // 1. Delete obsolete stores first
  operations.push(deleteObsoleteStores());
  
  // 2. Create or update stores and their indexes
  storesToCreate.forEach(({ name, options, indexes = [], obsoleteIndexes = [] }) => {
    operations.push(createOrUpdateStore(name, options, indexes, obsoleteIndexes));
  });
  
  // 3. Run migrations for existing schemas - no longer needed
  // operations.push(migrateImageCacheSchema());
  
  // 4. Seed initial data
  operations.push(seedInitialData());
  
  // Wait for all operations to complete
  Promise.all(operations)
    .then(() => {
      console.log("DB schema initialization/upgrade process completed successfully.");
    })
    .catch(error => {
      console.error("Error during DB schema initialization/upgrade:", error);
      throw error; // Propagate error to abort the transaction
    });
}

async function runScheduledBackup(db, userSettings) {
  if (!userSettings.backup_active) return;

  const now = new Date();
  const lastBackup = userSettings.last_backup ? new Date(userSettings.last_backup) : null;
  const scheduleTime = userSettings.backup_schedule ? new Date(userSettings.backup_schedule) : now;
  const interval = userSettings.backup_interval || 'daily';

  // Determine next scheduled backup time
  let nextBackup = scheduleTime;
  if (lastBackup) {
    switch (interval) {
      case 'daily':
        nextBackup = new Date(lastBackup.getTime() + 24 * 60 * 60 * 1000);
        break;
      case 'weekly':
        nextBackup = new Date(lastBackup.getTime() + 7 * 24 * 60 * 60 * 1000);
        break;
      case 'monthly':
        nextBackup = new Date(lastBackup.setMonth(lastBackup.getMonth() + 1));
        break;
      default:
        nextBackup = scheduleTime;
    }
  }

  // If it's time for a backup
  if (!lastBackup || now >= nextBackup) {
    await backupDatabase(db);
    userSettings.last_backup = new Date().toISOString();
    await saveUserSettingsToDB(userSettings, ['last_backup']);
  }
}

/**
 * Backup the entire IndexedDB database as a JSON file.
 */
async function backupDatabase(db) {
  try {
    const objectStoreNames = Array.from(db.objectStoreNames);
    if (objectStoreNames.length === 0) {
      showNotification("No data found in the database to back up.", false);
      return;
    }

    const timestamp = new Date().toISOString().replace(/[-:T]/g, "_").split(".")[0];
    const MAX_FILE_SIZE = 400 * 1024 * 1024; // 400MB per file

    // Create metadata for the entire backup
    const backupMetadata = {
      timestamp,
      databaseVersion: db.version,
      stores: objectStoreNames,
      totalFiles: 0,
      totalRecords: 0,
      totalStores: objectStoreNames.length
    };

    let totalRecords = 0;
    let fileIndex = 1; // Start from 1 instead of 0
    const totalStores = objectStoreNames.length;
    let currentStore = 0;
    let currentFile = {
      metadata: { ...backupMetadata },
      data: {}
    };

    // Process each store completely before moving to the next
    for (const storeName of objectStoreNames) {
      currentStore++;

      // Show detailed notification with store count and percentage
      const storePercent = Math.round(((currentStore - 1) / totalStores) * 100);
      showNotification(`Store ${currentStore}/${totalStores}: "${storeName}" (0% complete) - Overall: ${storePercent}% complete - File ${fileIndex}`, true);

      // Get all records for this store
      const transaction = db.transaction([storeName], "readonly");
      const store = transaction.objectStore(storeName);

      // Get the total count first
      const countRequest = store.count();
      const storeRecordCount = await new Promise((resolve, reject) => {
        countRequest.onsuccess = () => resolve(countRequest.result);
        countRequest.onerror = () => reject(new Error(`Error counting records in '${storeName}': ${countRequest.error}`));
      });

      if (storeRecordCount === 0) continue;

      // Initialize store in current file
      if (!currentFile.data[storeName]) {
        currentFile.data[storeName] = [];
      }

      // Process all records for this store
      let processedRecords = 0;
      let allRecords = [];

      // Fetch all records with progress reporting
      await new Promise((resolve, reject) => {
        const cursorRequest = store.openCursor();

        cursorRequest.onsuccess = (event) => {
          const cursor = event.target.result;
          if (cursor) {
            allRecords.push(cursor.value);
            processedRecords++;

            // Report progress every 500 records
            if (processedRecords % 500 === 0) {
              const storePercent = Math.round((processedRecords / storeRecordCount) * 100);
              const overallPercent = Math.round(((currentStore - 1) / totalStores + (processedRecords / storeRecordCount) / totalStores) * 100);
              showNotification(`Store ${currentStore}/${totalStores}: "${storeName}" - ${processedRecords}/${storeRecordCount} records (${storePercent}%) - Overall: ${overallPercent}% complete - File ${fileIndex}`, true);
            }

            cursor.continue();
          } else {
            // Final progress update
            const storePercent = Math.round((processedRecords / storeRecordCount) * 100);
            const overallPercent = Math.round(((currentStore - 1) / totalStores + (processedRecords / storeRecordCount) / totalStores) * 100);
            showNotification(`Store ${currentStore}/${totalStores}: "${storeName}" - ${processedRecords}/${storeRecordCount} records (${storePercent}%) - Overall: ${overallPercent}% complete - File ${fileIndex}`, true);
            resolve();
          }
        };

        cursorRequest.onerror = (event) => {
          reject(event.target.error);
        };
      });

      totalRecords += allRecords.length;

      // Check if adding this store would exceed the file size limit
      currentFile.data[storeName] = allRecords;
      const estimatedSize = JSON.stringify(currentFile).length;

      if (estimatedSize > MAX_FILE_SIZE && Object.keys(currentFile.data).length > 1) {
        // If this store alone exceeds the limit and there are other stores already in the file
        // Save the current file without this store
        delete currentFile.data[storeName];

        // Update metadata
        currentFile.metadata.fileIndex = fileIndex;
        currentFile.metadata.totalRecords = Object.values(currentFile.data).flat().length;

        // Save current file
        const jsonData = JSON.stringify(currentFile);
        const blob = new Blob([jsonData], { type: "application/json" });
        const url = URL.createObjectURL(blob);

        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = `${timestamp}_filmy-backup_${fileIndex}.json`;
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        URL.revokeObjectURL(url);

        // Add a small delay to prevent browser from being overwhelmed with downloads
        await new Promise(resolve => setTimeout(resolve, 300));

        // Start a new file with just this store
        fileIndex++;
        currentFile = {
          metadata: { ...backupMetadata },
          data: {
            [storeName]: allRecords
          }
        };
      } else if (estimatedSize > MAX_FILE_SIZE) {
        // If this store alone exceeds the limit, we need to split it across multiple files
        // Chunk the array into smaller arrays
        const chunks = [];
        const chunkSize = Math.ceil(allRecords.length / Math.ceil(estimatedSize / MAX_FILE_SIZE));
        for (let i = 0; i < allRecords.length; i += chunkSize) {
          chunks.push(allRecords.slice(i, i + chunkSize));
        }

        for (let i = 0; i < chunks.length; i++) {
          // For first chunk, use current file if it's empty
          if (i === 0 && Object.keys(currentFile.data).length === 1 && currentFile.data[storeName].length === 0) {
            currentFile.data[storeName] = chunks[i];
          } else {
            // Save current file if not empty
            if (Object.keys(currentFile.data).some(store => currentFile.data[store].length > 0)) {
              currentFile.metadata.fileIndex = fileIndex;
              currentFile.metadata.totalRecords = Object.values(currentFile.data).flat().length;

              const jsonData = JSON.stringify(currentFile);
              const blob = new Blob([jsonData], { type: "application/json" });
              const url = URL.createObjectURL(blob);

              const anchor = document.createElement("a");
              anchor.href = url;
              anchor.download = `${timestamp}_filmy-backup_${fileIndex}.json`;
              document.body.appendChild(anchor);
              anchor.click();
              document.body.removeChild(anchor);
              URL.revokeObjectURL(url);

              // Add a small delay to prevent browser from being overwhelmed with downloads
              await new Promise(resolve => setTimeout(resolve, 300));

              fileIndex++;
            }

            // Create new file with this chunk
            currentFile = {
              metadata: { ...backupMetadata },
              data: {
                [storeName]: chunks[i]
              }
            };
          }

          // If this is not the last chunk, save the file
          if (i < chunks.length - 1) {
            currentFile.metadata.fileIndex = fileIndex;
            currentFile.metadata.totalRecords = Object.values(currentFile.data).flat().length;
            currentFile.metadata.storeInfo = {
              [storeName]: {
                totalChunks: chunks.length,
                currentChunk: i + 1,
                totalRecords: allRecords.length
              }
            };

            const jsonData = JSON.stringify(currentFile);
            const blob = new Blob([jsonData], { type: "application/json" });
            const url = URL.createObjectURL(blob);

            const anchor = document.createElement("a");
            anchor.href = url;
            anchor.download = `${timestamp}_filmy-backup_${fileIndex}.json`;
            document.body.appendChild(anchor);
            anchor.click();
            document.body.removeChild(anchor);
            URL.revokeObjectURL(url);

            // Add a small delay to prevent browser from being overwhelmed with downloads
            await new Promise(resolve => setTimeout(resolve, 300));

            fileIndex++;
          }
        }
      }

      // If we've reached the last store, save the current file
      if (currentStore === totalStores) {
        currentFile.metadata.fileIndex = fileIndex;
        currentFile.metadata.totalFiles = fileIndex;
        currentFile.metadata.totalRecords = Object.values(currentFile.data).flat().length;

        const jsonData = JSON.stringify(currentFile);
        const blob = new Blob([jsonData], { type: "application/json" });
        const url = URL.createObjectURL(blob);

        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = `${timestamp}_filmy-backup_${fileIndex}.json`;
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        URL.revokeObjectURL(url);

        // Add a small delay to prevent browser from being overwhelmed with downloads
        await new Promise(resolve => setTimeout(resolve, 300));
      }
    }

    showNotification(`Database backed up successfully: ${totalRecords} records in ${fileIndex} files with timestamp "${timestamp}"`, false);
  } catch (error) {
    console.error("Error during backup:", error);
    showNotification(`Backup error: ${error.message}`, false);
  }
}

async function restoreDatabase(files, db) {
  try {
    // Sort files to ensure they're processed in order
    const sortedFiles = Array.from(files).sort((a, b) => {
      return a.name.localeCompare(b.name);
    });

    if (sortedFiles.length === 0) {
      showNotification("No backup files selected.", false);
      return false;
    }

    // Find the file with the highest non-zero totalFiles value
    let expectedTotalFiles = 0;
    for (const file of sortedFiles) {
      const fileContent = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (event) => {
          try {
            resolve(JSON.parse(event.target.result));
          } catch {
            reject(new Error(`Invalid JSON format in file ${file.name}.`));
          }
        };
        reader.onerror = () => reject(new Error(`Error reading file ${file.name}.`));
        reader.readAsText(file);
      });
      
      if (fileContent.metadata && fileContent.metadata.totalFiles > expectedTotalFiles) {
        expectedTotalFiles = fileContent.metadata.totalFiles;
      }
    }

    // Check if we have all required files
    if (expectedTotalFiles === 0) {
      showNotification("Missing the final backup file which contains total file count. Please select all backup files.", false);
      return false;
    } else if (expectedTotalFiles > sortedFiles.length) {
      showNotification(`This backup consists of ${expectedTotalFiles} files, but only ${sortedFiles.length} were selected. Please select all backup files.`, false);
      return false;
    }

    // Read the first file to get metadata for version check
    const firstFileContent = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (event) => {
        try {
          resolve(JSON.parse(event.target.result));
        } catch {
          reject(new Error("Invalid JSON format in first file."));
        }
      };
      reader.onerror = () => reject(new Error("Error reading the first file."));
      reader.readAsText(sortedFiles[0]);
    });

    const metadata = firstFileContent.metadata;
    if (!metadata) {
      showNotification("Invalid backup file format. Missing metadata.", false);
      return false;
    }

    // Check database version
    if (metadata.databaseVersion !== db.version) {
      showNotification(
        `Warning: Backup DB version (${metadata.databaseVersion}) ≠ Current DB version (${db.version})`,
        false
      );
    }

    // Track which stores we've already processed to handle split stores
    const processedStores = new Set();
    const storeChunks = {};

    // Count total stores across all files for progress tracking
    let totalStores = 0;
    let allStoreNames = new Set();

    // First pass to count unique stores
    for (let i = 0; i < sortedFiles.length; i++) {
      const fileContent = i === 0 ? firstFileContent : await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (event) => {
          try {
            resolve(JSON.parse(event.target.result));
          } catch {
            reject(new Error(`Invalid JSON format in file ${sortedFiles[i].name}.`));
          }
        };
        reader.onerror = () => reject(new Error(`Error reading file ${sortedFiles[i].name}.`));
        reader.readAsText(sortedFiles[i]);
      });

      if (fileContent.data) {
        Object.keys(fileContent.data).forEach(storeName => {
          if (!allStoreNames.has(storeName)) {
            allStoreNames.add(storeName);
            totalStores++;
          }
        });
      }
    }

    let currentStoreIndex = 0;

    showNotification(`Found backup with ${sortedFiles.length} files. Starting restore...`, true);

    // Process each file
    for (let i = 0; i < sortedFiles.length; i++) {
      const file = sortedFiles[i];
      showNotification(`Restoring file ${i + 1}/${sortedFiles.length}...`, true);

      // Read file content (reuse first file content if it's the first file)
      const fileContent = i === 0 ? firstFileContent : await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (event) => {
          try {
            resolve(JSON.parse(event.target.result));
          } catch {
            reject(new Error(`Invalid JSON format in file ${file.name}.`));
          }
        };
        reader.onerror = () => reject(new Error(`Error reading file ${file.name}.`));
        reader.readAsText(file);
      });

      // Check if file has data property
      if (!fileContent.data || Object.keys(fileContent.data).length === 0) {
        console.warn(`File ${file.name} contains no data to restore.`);
        continue;
      }

      // Check if this file contains store chunks
      const hasStoreChunks = fileContent.metadata && fileContent.metadata.storeInfo;

      // Process each store in the file
      const storeEntries = Object.entries(fileContent.data);
      for (let storeIdx = 0; storeIdx < storeEntries.length; storeIdx++) {
        const [storeName, records] = storeEntries[storeIdx];

        // Skip if already processed (for non-chunked stores)
        if (!hasStoreChunks && processedStores.has(storeName)) {
          continue;
        }

        // Only increment for stores we haven't processed yet
        if (!processedStores.has(storeName) && (!hasStoreChunks || !storeChunks[storeName])) {
          currentStoreIndex++;
        }

        if (!db.objectStoreNames.contains(storeName)) {
          console.warn(`Skipping store "${storeName}" as it doesn't exist in the current database.`);
          continue;
        }

        if (!Array.isArray(records) || records.length === 0) {
          console.warn(`Skipping empty or invalid records for store "${storeName}".`);
          continue;
        }

        // Explicitly remove any existing progress notification before starting a new store
        removeNotification(note => note.isProgress && !note.isBackgroundTask);

        // Calculate overall progress
        const storeProgress = Math.round((currentStoreIndex / totalStores) * 100);

        // If this is a chunked store, handle differently
        if (hasStoreChunks && fileContent.metadata.storeInfo[storeName]) {
          const chunkInfo = fileContent.metadata.storeInfo[storeName];

          // Initialize store chunks if not already done
          if (!storeChunks[storeName]) {
            storeChunks[storeName] = {
              chunks: Array(chunkInfo.totalChunks).fill(null),
              totalRecords: chunkInfo.totalRecords,
              processedChunks: 0
            };
          }

          // Store chunk reference instead of actual data to save memory
          storeChunks[storeName].chunks[chunkInfo.currentChunk - 1] = {
            fileIndex: i,
            records: records
          };
          storeChunks[storeName].processedChunks++;

          // Show progress for chunked store
          showNotification(
            `Store ${currentStoreIndex}/${totalStores} (${storeProgress}%): "${storeName}" - Processing chunk ${storeChunks[storeName].processedChunks}/${chunkInfo.totalChunks} - File ${i + 1}/${sortedFiles.length}`,
            true
          );

          // If we've processed all chunks for this store, restore it
          if (storeChunks[storeName].processedChunks === chunkInfo.totalChunks) {
            // Clear the store before restoring
            await new Promise((resolve) => {
              const clearReq = db
                .transaction([storeName], "readwrite")
                .objectStore(storeName)
                .clear();
              clearReq.onsuccess = resolve;
              clearReq.onerror = () => {
                console.warn(`Failed to clear store "${storeName}"`);
                resolve(); // Continue anyway
              };
            });

            // Process each chunk directly without flattening the entire array
            const totalRecords = chunkInfo.totalRecords;
            let processedRecords = 0;

            // Process each chunk separately
            for (let chunkIdx = 0; chunkIdx < storeChunks[storeName].chunks.length; chunkIdx++) {
              const chunkRef = storeChunks[storeName].chunks[chunkIdx];
              const chunkRecords = chunkRef.records;

              // Process this chunk in smaller batches
              const batchSize = 500;
              for (let offset = 0; offset < chunkRecords.length; offset += batchSize) {
                const batch = chunkRecords.slice(offset, offset + batchSize);

                await new Promise(resolve => {
                  const transaction = db.transaction([storeName], "readwrite");
                  const store = transaction.objectStore(storeName);
                  let completed = 0;

                  batch.forEach(record => {
                    try {
                      const req = store.put(record);
                      req.onsuccess = () => {
                        completed++;
                        if (completed === batch.length) resolve();
                      };
                      req.onerror = (event) => {
                        console.warn(`Error restoring record in "${storeName}": ${event.target.error}`);
                        completed++;
                        if (completed === batch.length) resolve();
                      };
                    } catch (error) {
                      console.error(`Exception restoring record: ${error}`);
                      completed++;
                      if (completed === batch.length) resolve();
                    }
                  });

                  transaction.onerror = (event) => {
                    console.error(`Transaction error restoring chunk in "${storeName}":`, event.target.error);
                    resolve(); // Continue with next batch
                  };
                });

                processedRecords += batch.length;
                const percent = Math.round((processedRecords / totalRecords) * 100);

                // Remove previous notification and show updated one
                removeNotification(note => note.isProgress && !note.isBackgroundTask);

                // Enhanced notification with total progress information
                showNotification(
                  `Store ${currentStoreIndex}/${totalStores} (${storeProgress}%): "${storeName}" - ${processedRecords}/${totalRecords} records (${percent}%) - File ${i + 1}/${sortedFiles.length}`,
                  true
                );

                // Yield to UI
                await new Promise(res => setTimeout(res, 0));
              }

              // Free memory by clearing the reference after processing
              storeChunks[storeName].chunks[chunkIdx] = null;
            }

            processedStores.add(storeName);
            console.log(`Restored chunked store "${storeName}" with ${totalRecords} records`);
          }
        }
        // If this store hasn't been processed yet and isn't chunked, restore it directly
        else if (!processedStores.has(storeName)) {
          console.log(`Starting restore of store "${storeName}" with ${records.length} records`);

          // Show initial notification for this store
          showNotification(
            `Store ${currentStoreIndex}/${totalStores} (${storeProgress}%): "${storeName}" - 0/${records.length} records (0%) - File ${i + 1}/${sortedFiles.length}`,
            true
          );

          // Clear the store before restoring
          await new Promise((resolve) => {
            const clearReq = db
              .transaction([storeName], "readwrite")
              .objectStore(storeName)
              .clear();
            clearReq.onsuccess = resolve;
            clearReq.onerror = () => {
              console.warn(`Failed to clear store "${storeName}"`);
              resolve(); // Continue anyway
            };
          });

          // Determine if this is a large store that needs special handling
          const isLargeStore = records.length > 10000;
          const chunkSize = isLargeStore ? 1000 : 500; // Larger batch size for large stores

          // Restore records in chunks
          let restored = 0;
          while (restored < records.length) {
            const chunk = records.slice(restored, restored + chunkSize);
            await new Promise(resolve => {
              const transaction = db.transaction([storeName], "readwrite");
              const store = transaction.objectStore(storeName);

              let completed = 0;

              chunk.forEach(record => {
                try {
                  const req = store.put(record);
                  req.onsuccess = () => {
                    completed++;
                    if (completed === chunk.length) resolve();
                  };
                  req.onerror = (event) => {
                    console.warn(`Error restoring record in "${storeName}": ${event.target.error}`);
                    completed++;
                    if (completed === chunk.length) resolve();
                  };
                } catch (error) {
                  console.error(`Exception restoring record: ${error}`);
                  completed++;
                  if (completed === chunk.length) resolve();
                }
              });

              transaction.onerror = (event) => {
                console.error(`Transaction error restoring chunk in "${storeName}":`, event.target.error);
                resolve(); // Continue with next chunk
              };
            });

            restored += chunk.length;
            const percent = Math.round((restored / records.length) * 100);

            // Remove previous notification and show updated one
            removeNotification(note => note.isProgress && !note.isBackgroundTask);

            // Enhanced notification with total progress information
            showNotification(
              `Store ${currentStoreIndex}/${totalStores} (${storeProgress}%): "${storeName}" - ${restored}/${records.length} records (${percent}%) - File ${i + 1}/${sortedFiles.length}`,
              true
            );

            // Yield to UI
            await new Promise(res => setTimeout(res, 0));
          }

          processedStores.add(storeName);
          console.log(`Completed restore of store "${storeName}"`);
        }

        // Add a small delay after processing each store to ensure notification is visible
        await new Promise(resolve => setTimeout(resolve, 50));
      }

      // Allow UI to update
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    // Check if any chunked stores are incomplete
    let incompleteStores = 0;
    for (const [storeName, info] of Object.entries(storeChunks)) {
      if (info.processedChunks < info.chunks.length) {
        showNotification(`Warning: Store "${storeName}" is incomplete. Only ${info.processedChunks} of ${info.chunks.length} chunks were restored.`, false);
        incompleteStores++;
      }
    }

    if (processedStores.size > 0) {
      if (incompleteStores > 0) {
        showNotification(`Database restore completed with ${incompleteStores} incomplete stores. Some data may be missing.`, false);
      } else {
        showNotification(`Database restore completed successfully! Restored ${processedStores.size} stores.`, false);
      }
    } else {
      showNotification("No data was restored. Check that the backup files contain valid data.", false);
    }
    return true;
  } catch (error) {
    console.error("Error during restore:", error);
    showNotification(`Restore error: ${error.message}`, false);
    return false;
  }
}

async function fetchAllRecords(db) {
  try {
    // Get store names and prepare maps
    const storeNames = ["movies", "series", "links", "lists", "movie_details", "series_details"];
    const linksMap = {};
    const listsMap = {};
    const detailsMap = {};
    const allMedia = [];
    
    // Process each store in chunks
    for (const storeName of storeNames) {
      const data = await fetchStoreInChunks(db, storeName);
      
      // Process data based on store type
      if (storeName === "links") {
        for (const link of data) {
          if (!linksMap[link.imdbID]) linksMap[link.imdbID] = [];
          linksMap[link.imdbID].push(link);
        }
      } else if (storeName === "lists") {
        for (const listEntry of data) {
          if (
            listEntry.username === userSettings.username &&
            listEntry.imdbID &&
            listEntry.list
          ) {
            if (!listsMap[listEntry.imdbID]) listsMap[listEntry.imdbID] = {};
            listsMap[listEntry.imdbID][listEntry.list] = true;
          }
        }
      } else if (storeName === "movie_details" || storeName === "series_details") {
        for (const detail of data) {
          detailsMap[detail.tmdbID] = detail;
        }
      } else if (storeName === "movies") {
        for (const movie of data) {
          allMedia.push({ ...movie, mediaType: 'movie' });
        }
      } else if (storeName === "series") {
        for (const series of data) {
          allMedia.push({ ...series, mediaType: 'tv' });
        }
      }
      
      // Allow UI to update between stores
      await new Promise(resolve => setTimeout(resolve, 0));
    }

    // Process media in chunks to merge with details
    const mergedMedia = await processMediaInChunks(allMedia, linksMap, listsMap, detailsMap);
    
    console.log(`Fetched ${mergedMedia.length} records (${allMedia.filter(m => m.mediaType === 'movie').length} movies + ${allMedia.filter(m => m.mediaType === 'tv').length} series)`);
    return convertYearToNumber(mergedMedia);
  } catch (error) {
    console.error("Error fetching data:", error);
    throw error;
  }
}

async function fetchStoreInChunks(db, storeName) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(storeName, "readonly");
    const store = transaction.objectStore(storeName);
    const results = [];
    
    // Get count first to determine approach
    const countRequest = store.count();
    countRequest.onsuccess = () => {
      const totalCount = countRequest.result;
      
      // For small stores (< 1000 items), just use getAll
      if (totalCount < 1000) {
        const getAllRequest = store.getAll();
        getAllRequest.onsuccess = () => resolve(getAllRequest.result);
        getAllRequest.onerror = (e) => reject(e.target.error);
        return;
      }
      
      // For larger stores, use cursor with chunking
      const chunkSize = 500;
      let processed = 0;
      
      const cursorRequest = store.openCursor();
      cursorRequest.onsuccess = (event) => {
        const cursor = event.target.result;
        if (!cursor) {
          resolve(results);
          return;
        }
        
        results.push(cursor.value);
        processed++;
        
        // Continue to next record
        cursor.continue();
        
        // Yield to UI thread periodically
        if (processed % chunkSize === 0) {
          setTimeout(() => {
            // This timeout allows the UI thread to process
          }, 0);
        }
      };
      
      cursorRequest.onerror = (e) => reject(e.target.error);
    };
    
    countRequest.onerror = (e) => reject(e.target.error);
  });
}

async function processMediaInChunks(allMedia, linksMap, listsMap, detailsMap) {
  const mergedMedia = [];
  const chunkSize = 500;
  const totalChunks = Math.ceil(allMedia.length / chunkSize);
  
  for (let i = 0; i < totalChunks; i++) {
    const startIndex = i * chunkSize;
    const endIndex = Math.min(startIndex + chunkSize, allMedia.length);
    const chunk = allMedia.slice(startIndex, endIndex);
    
    // Process this chunk
    for (const media of chunk) {
      const detail = detailsMap[media.tmdbID] || {};
      
      // Extract numeric year from string year
      let numericYear = null;
      if (media.Year) {
        const yearMatch = media.Year.match(/^(\d{4})/);
        numericYear = yearMatch ? parseInt(yearMatch[1], 10) : null;
      }
      
      mergedMedia.push({
        ...media,
        numericYear,
        defaultPoster: media.Poster,
        Poster: getMediaCustomization(media.imdbID, 'poster') || media.Poster,
        defaultBackdrop: media.backdrop_path,
        backdrop_path: getMediaCustomization(media.imdbID, 'backdrop') || media.backdrop_path,
        links: linksMap[media.imdbID] || [],
        lists: listsMap[media.imdbID] || {},
        origin_country: detail.origin_country || [],
        trailers: (detail.videos?.results || []).filter(
          v => v.type === "Trailer" && v.site === "YouTube" && v.status !== 404 // filter out private and dead YT links
        )
      });
    }
    
    // Allow UI to update between chunks
    if (i < totalChunks - 1) {
      await new Promise(resolve => setTimeout(resolve, 0));
    }
  }
  
  return mergedMedia;
}

async function applyFiltersAndSearch(shouldSort = true, preFilteredResults = null, shouldResetGrid = true) {
  // Start with the prefiltered results if provided; otherwise, use all movies from mediaCache
  let filtered = preFilteredResults ? preFilteredResults : Array.from(mediaCache.values());

  // Apply media type filter if active
  if (activeMediaType) {
    filtered = filtered.filter(media => media.mediaType === activeMediaType);
  }

  // Apply search if needed
  if (!preFilteredResults && activeSearchQuery && activeSearchQuery.trim() !== "") {
    filtered = searchItems(filtered, activeSearchQuery, ["Title", "original_title", "dataTitle"]);
  }

  // Combine all filters in a single pass for better performance
  if (activeLists.length > 0 ||
    activeGenres.length > 0 ||
    activeMinImdb !== null || activeMaxImdb !== null ||
    activeMinTmdb !== null || activeMaxTmdb !== null ||
    activeMinMetascore !== null || activeMaxMetascore !== null ||
    activeMinRT !== null || activeMaxRT !== null ||
    (activeCountry && activeCountry !== "All")) {

    // Precompute filter conditions
    const minRating = activeMinImdb !== null ? activeMinImdb : -Infinity;
    const maxRating = activeMaxImdb !== null ? activeMaxImdb : Infinity;
    const minRatingTmdb = activeMinTmdb !== null ? activeMinTmdb : -Infinity;
    const maxRatingTmdb = activeMaxTmdb !== null ? activeMaxTmdb : Infinity;
    const minMetascore = activeMinMetascore !== null ? activeMinMetascore : -Infinity;
    const maxMetascore = activeMaxMetascore !== null ? activeMaxMetascore : Infinity;
    const minRT = activeMinRT !== null ? activeMinRT : -Infinity;
    const maxRT = activeMaxRT !== null ? activeMaxRT : Infinity;

    // Use reduce instead of multiple filter calls for better performance
    filtered = filtered.reduce((result, media) => {
      // List filter (supports both core and custom lists)
      if (activeLists.length > 0) {
        const passesListFilter = activeLists.some(({ name, type }) => {
          if (type === 'core') {
            if (name === "watched") {
              return media.lists && media.lists.watched === true;
            } else if (name === "unwatched") {
              return !(media.lists && media.lists.watched === true);
            } else {
              return media.lists && media.lists[name] === true;
            }
          } else if (type === 'custom') {
            // Custom lists: match by UUID
            return media.lists && media.lists[name] === true;
          }
          return false;
        });
        if (!passesListFilter) return result;
      }

      // Genre filter
      if (activeGenres.length > 0) {
        if (!activeGenres.every(genreID => media.genre_ids && media.genre_ids.includes(genreID))) {
          return result;
        }
      }

      // IMDb rating filter
      if (activeMinImdb !== null || activeMaxImdb !== null) {
        const imdbRatingObj = media.Ratings?.find(
          rating => rating.Source === "Internet Movie Database"
        );
        const imdbValue = imdbRatingObj ? parseFloat(imdbRatingObj.Value.split("/")[0]) : null;
        if (imdbValue === null || imdbValue < minRating || imdbValue > maxRating) {
          return result;
        }
      }

      // TMDB rating filter
      if (activeMinTmdb !== null || activeMaxTmdb !== null) {
        const tmdbValue = media.vote_average ? Math.round(media.vote_average * 10) / 10 : null;
        if (tmdbValue === null || tmdbValue < minRatingTmdb || tmdbValue > maxRatingTmdb) {
          return result;
        }
      }

      // Metascore filter
      if (activeMinMetascore !== null || activeMaxMetascore !== null) {
        const metascoreValue = media.Metascore && media.Metascore !== "N/A"
          ? parseInt(media.Metascore, 10)
          : null;
        if (metascoreValue === null || metascoreValue < minMetascore || metascoreValue > maxMetascore) {
          return result;
        }
      }

      // Rotten Tomatoes filter
      if (activeMinRT !== null || activeMaxRT !== null) {
        const rtRatingObj = media.Ratings?.find(
          rating => rating.Source === "Rotten Tomatoes"
        );
        const rtValue = rtRatingObj ? parseInt(rtRatingObj.Value.replace("%", ""), 10) : null;
        if (rtValue === null || rtValue < minRT || rtValue > maxRT) {
          return result;
        }
      }

      // Country filter
      if (activeCountry && activeCountry !== "All") {
        if (!Array.isArray(media.origin_country) || !media.origin_country.includes(activeCountry)) {
          return result;
        }
      }

      // All filters passed, add to result
      result.push(media);
      return result;
    }, []);
  }

  // Apply year range filter using the "Year"
  if (
    activeStartYear !== null &&
    activeEndYear !== null &&
    (activeStartYear !== defaultStartYear || activeEndYear !== defaultEndYear)
  ) {
    // Filter by year range
    filtered = filtered.filter(item => {
      // Ensure we have a numeric year to filter by
      const year = Number(item.Year) || 0;
      return year >= activeStartYear && year <= activeEndYear;
    });

    // Update year filter button appearance to show filter is active
    const yearFilterButton = document.getElementById('year-filter-button');
    
    // Only update the filter active state, not sort indicators
    if (!yearFilterButton.classList.contains('sort-asc') && !yearFilterButton.classList.contains('sort-desc')) {
      yearFilterButton.classList.add('sort-active');
    }
  } else {
    // If year filter is not active, remove the active state but preserve sort indicators
    const yearFilterButton = document.getElementById('year-filter-button');
    if (!yearFilterButton.classList.contains('sort-asc') && !yearFilterButton.classList.contains('sort-desc')) {
      yearFilterButton.classList.remove('sort-active');
    }
  }
 
  // Apply sorting based on current state
  if (shouldSort) {
    if (currentSortField === "year") {
      // Only sort by year if explicitly selected via year sort button
      filtered = sortRecords(filtered, "year", yearSortAscending);
    } else if (currentSortField && currentSortField !== "title") {
      // Apply rating sort if active
      filtered = sortRecords(filtered, currentSortField, currentSortOrder === 'asc');
    } else {
      // Default to alphabetical sorting by title
      filtered = sortRecords(filtered, "title", true);
      if (!currentSortField) currentSortField = "title";
    }
  }
  
  // Update the global filteredRecords array
  filteredRecords = filtered;

  // Reset lazy loading and reload movies in batches
  if (shouldResetGrid) {
    resetGrid(true);
  }
  loadPaginatedRecords(startIndex);

  // Smooth-scroll to the top of the movie grid
  requestAnimationFrame(() => {
    scrollToTopOfGrid();
  });

  // Update the match count display
  updateMatchCount();
}

/**
 * Scroll to ensure that the movie grid starts exactly below the menu bar.
 */
function scrollToTopOfGrid() {
  const movieGrid = document.querySelector(".card-grid");
  const menuBarHeight = getMenuBarHeight();

  if (movieGrid) {
    const offsetTop = movieGrid.getBoundingClientRect().top + window.scrollY - menuBarHeight;
    window.scrollTo({ top: offsetTop, behavior: "auto" }); // Use "auto" for instant alignment
    // console.log("Scrolled to top of movie grid, adjusted for menu bar.");
  } else {
    console.warn("Movie grid not found for scrolling.");
  }
}

/**
 * Update the match count display.
 */
function updateMatchCount() {
  const matchCountElement = document.getElementById("match-count");
  if (!matchCountElement) return;

  const filteredElement = matchCountElement.querySelector(".filtered-records");
  const totalElement = matchCountElement.querySelector(".total-records");
  const tooltipElement = matchCountElement.querySelector(".ttip-txt");

  if (filteredElement && totalElement && tooltipElement) {

    const movieCount = filteredRecords.filter(item => item.mediaType === 'movie').length;
    const tvCount = filteredRecords.filter(item => item.mediaType === 'tv').length;
    
    filteredElement.textContent = filteredRecords.length.toLocaleString();
    totalElement.textContent = totalRecords.toLocaleString();

    // Construct tooltip content based on activeMediaType
    let tooltipContent = '<div class="tooltip-content"><div class="count-table">';

    if (activeMediaType === "movie") {
      // Only show movie count
      tooltipContent += `
        <div class="count-row">
          <span class="count-label">Movies</span>
          <span class="count-value">${movieCount.toLocaleString()}</span>
        </div>`;
    } else if (activeMediaType === "tv") {
      // Only show TV show count
      tooltipContent += `
        <div class="count-row">
          <span class="count-label">Series</span>
          <span class="count-value">${tvCount.toLocaleString()}</span>
        </div>`;
    } else {
      // Show both counts
      tooltipContent += `
        <div class="count-row">
          <span class="count-label">Movies</span>
          <span class="count-value">${movieCount.toLocaleString()}</span>
        </div>
        <div class="count-row">
          <span class="count-label">Series</span>
          <span class="count-value">${tvCount.toLocaleString()}</span>
        </div>`;
    }

    tooltipElement.innerHTML = tooltipContent;

    // Apply styling if filtered count is less than total
    if (filteredRecords.length < totalRecords) {
      filteredElement.classList.add("highlight");
      matchCountElement.classList.add("active"); 
    } else {
      filteredElement.classList.remove("highlight");
      matchCountElement.classList.remove("active"); 
    }
  }
}

/**
 * DB: Load all movies into memory
 * @param {*} db 
 * @returns 
 */
async function loadRecords(db) {
  if (isFetching) return;
  isFetching = true;

  try {
    const allRecords = await fetchAllRecords(db);

    await reloadListMetadata(userSettings.username);
    cacheRecords(allRecords);
    totalRecords = allRecords.length;
    populateCountryDropdown(allRecords); 
    updateActiveYears(); 

    filteredRecords = sortRecords([...allRecords], "title", true);
    resetGrid(true);
    loadPaginatedRecords(0);
    updateMatchCount();

  } catch (error) {
    console.error("Error loading records:", error);
  } finally {
    isFetching = false;
  }
}

/**
 * Loads all list metadata and updates global arrays.
 * Call this after any change to lists_metadata, and on app load.
 */
async function reloadListMetadata(username) {
  allListsMeta = await getUserLists(username);
  customListsMeta = allListsMeta.filter(list =>
    list.type === 'custom' &&
    (list.username === username || list.username === 'default')
  );
  
  // Update listsMap with the latest metadata
  const tempMap = {};
  allListsMeta.forEach(list => {
    tempMap[list.list] = list;
  });
  listsMap = tempMap;
}

/**
 * Update the active start and end years based on the current movie cache.
 */
function updateActiveYears() {
  if (mediaCache.size > 0) {
    // Create (or reuse) a sorted cache of movies by numericYear.
    const sortedCache = [...mediaCache.values()].sort((a, b) => a.numericYear - b.numericYear);
    // Set global active years to the minimum and maximum found in the dataset.
    activeStartYear = sortedCache[0].numericYear;
    activeEndYear = sortedCache[sortedCache.length - 1].numericYear;
    defaultStartYear = activeStartYear;
    defaultEndYear = activeEndYear;
  } else {
    // Reset to default values if the cache is empty
    activeStartYear = null;
    activeEndYear = null;
    defaultStartYear = null;
    defaultEndYear = null;
  }
}

function cacheRecords(records) {
  if (!Array.isArray(records)) {
    console.error("Invalid input to cacheMovies: expected an array, got", records);
    return;
  }

  for (const title of records) {
    mediaCache.set(title.imdbID, {
      ...title,
      mediaType: title.mediaType
    });
  }

  // console.log(`Cached ${records.length} records (${movieCount} movies + ${tvCount} series)`);
}

async function updateCache(imdbID, db, mediaType) {
  const storeName = mediaType === 'tv' ? 'series' : 'movies';
  const detailsStoreName = mediaType === 'tv' ? 'series_details' : 'movie_details';

  const transaction = db.transaction([storeName, "links", "lists"], "readonly");

  const record = await new Promise((resolve, reject) => {
    const req = transaction.objectStore(storeName).get(imdbID);
    req.onsuccess = (e) => resolve(e.target.result);
    req.onerror = (e) => reject(new Error(`${mediaType} fetch error: ${e.target.error}`));
  });

  if (!record) {
    throw new Error(`${mediaType} with IMDb ID "${imdbID}" not found`);
  }

  // Fetch all links for the movie.
  const links = await new Promise((resolve, reject) => {
    const req = transaction.objectStore("links").index("imdbID").getAll(imdbID);
    req.onsuccess = (e) => resolve(e.target.result || []);
    req.onerror = (e) => reject(new Error(`Links fetch error: ${e.target.error}`));
  });

  // Fetch all lists for the movie.
  const lists = await new Promise((resolve, reject) => {
    const req = transaction.objectStore("lists").index("imdbID").getAll(imdbID);
    req.onsuccess = (e) => resolve(e.target.result || []);
    req.onerror = (e) => reject(new Error(`Lists fetch error: ${e.target.error}`));
  });

  // Transform lists data into an object.
  const validLists = Array.isArray(lists) ? lists : [];
  listsMap[imdbID] = validLists.reduce((acc, list) => {
    if (list && list.name && list.value !== undefined) {
      acc[list.name] = list.value;
    }
    return acc;
  }, {});

  const mergedData = {
    ...record,
    mediaType: mediaType,
    defaultPoster: record.Poster,
    Poster: getMediaCustomization(record.imdbID, 'poster') || record.Poster,
    defaultBackdrop: record.backdrop_path,
    backdrop_path: getMediaCustomization(record.imdbID, 'backdrop') || record.backdrop_path,
    links: Array.isArray(links) ? links : [],
    lists: listsMap[imdbID] || {},
    numericYear: record.Year
    ? (() => {
        const match = record.Year.match(/^(\d{4})/);
        return match ? parseInt(match[1], 10) : null;
      })()
    : null,
  };

  try {
    const details = await new Promise((resolve, reject) => {
      const detailsTransaction = db.transaction([detailsStoreName], "readonly");
      const detailsStore = detailsTransaction.objectStore(detailsStoreName);
      const detailsReq = detailsStore.get(record.tmdbID);
      detailsReq.onsuccess = (e) => resolve(e.target.result);
      detailsReq.onerror = (e) => reject(new Error(`${mediaType} details fetch error: ${e.target.error}`));
    });

    mergedData.origin_country = details && details.origin_country ? details.origin_country : [];
    mergedData.trailers = (details?.videos?.results || []).filter(
      v => v.type === "Trailer" && v.site === "YouTube" && v.status !== 404
    );
  } catch (error) {
    console.error(`Error fetching ${mediaType} details:`, error);
    mergedData.origin_country = [];
    mergedData.trailers = [];
  }

  // Update the global cache
  mediaCache.set(imdbID, mergedData); 
  
  console.log(`Updated cache for ${mediaType} IMDb ID "${imdbID}"`);
  return mergedData;
}

/**
 * Remove a movie from the cache by IMDb ID.
 * @param {string} imdbID - The IMDb ID of the movie to remove.
 */
function removeFromMediaCache(imdbID) {
  if (mediaCache.has(imdbID)) {
    mediaCache.delete(imdbID);
    console.log(`Removed movie with IMDb ID "${imdbID}" from the cache.`);
  } else {
    console.warn(`Movie with IMDb ID "${imdbID}" not found in the cache.`);
  }
}

/**
 * Updates the background color of the custom slider track based on the selected range.
 * @param {HTMLInputElement} rangeStart - The start range input element.
 * @param {HTMLInputElement} rangeEnd - The end range input element.
 * @param {HTMLElement} track - The custom slider track element.
 * @param {number} min - The minimum value of the slider.
 * @param {number} max - The maximum value of the slider.
 */
function updateYearSliderTrack(rangeStart, rangeEnd, track, min, max) {
  const startValue = parseInt(rangeStart.value, 10);
  const endValue = parseInt(rangeEnd.value, 10);

  // Calculate percentages for start and end values relative to min and max
  const startPercentage = ((startValue - min) / (max - min)) * 100;
  const endPercentage = ((endValue - min) / (max - min)) * 100;

  // Update the background gradient of the custom track
  track.style.background = `linear-gradient(to right, #3d3d3d ${startPercentage}%, #5d5d5d ${startPercentage}%, #5d5d5d ${endPercentage}%, #3d3d3d ${endPercentage}%)`;
}

/**
 * Update slider values and apply filters.
 */
function updateYearSliderValues(triggeredBy) {
  if (!yearRangeMin || !yearRangeMax) return;

  let startValue = parseInt(yearRangeMin.value, 10);
  let endValue = parseInt(yearRangeMax.value, 10);

  // Prevent overlap but allow pushing
  if (startValue > endValue) {
    if (yearRangeMin === document.activeElement) {
      // Min slider is active, so it pushes max slider
      endValue = startValue;
      yearRangeMax.value = endValue;
    } else {
      // Max slider is active, so it pushes min slider
      startValue = endValue;
      yearRangeMin.value = startValue;
    }
  }

  // Update labels
  startYearLabel.textContent = startValue;
  endYearLabel.textContent = endValue;

  // Update global variables for the active range
  activeStartYear = startValue;
  activeEndYear = endValue;

  // Update custom track colors dynamically
  updateYearSliderTrack(yearRangeMin, yearRangeMax, yearSliderTrack, minYear, maxYear);

  // Update thumb appearance but don't change sort
  if (startValue === minYear && endValue === maxYear) {
    // Reset thumb colors if sliders are at default positions
    yearRangeMin.classList.remove("active");
    yearRangeMax.classList.remove("active");
  } else if (triggeredBy === "min") {
    yearRangeMin.classList.add("active"); // Highlight min slider thumb
    yearRangeMax.classList.remove("active"); // Reset max slider thumb
  } else if (triggeredBy === "max") {
    yearRangeMax.classList.add("active"); // Highlight max slider thumb
    yearRangeMin.classList.remove("active"); // Reset min slider thumb
  }

  // Apply filtering with debounce - don't pass triggeredBy to avoid sort changes
  debouncedApplyFilters();

  // Update button state dynamically
  updateFilterButtonState(document.getElementById("year-filter-button"));
}

/**
 * Initialize the year range slider and set up event listeners.
 */
function initializeYearSlider() {
  yearRangeMin = document.getElementById("year-range-min");
  yearRangeMax = document.getElementById("year-range-max");
  startYearLabel = document.getElementById("start-year");
  endYearLabel = document.getElementById("end-year");
  yearSliderTrack = document.getElementById("year-slider-track");

  const yearFilterButton = document.getElementById("year-filter-button");
  minYear = activeStartYear;
  maxYear = activeEndYear;

  // Check if default years are available
  if (defaultStartYear === null || defaultEndYear === null) {
    yearFilterButton.disabled = true;
    yearFilterButton.classList.add("disabled");
    return;
  } else {
    yearFilterButton.disabled = false;
    yearFilterButton.classList.remove("disabled");
  }

  // Initialize sliders and labels with default values
  yearRangeMin.min = minYear;
  yearRangeMin.max = maxYear;
  yearRangeMin.value = activeStartYear || minYear;

  yearRangeMax.min = minYear;
  yearRangeMax.max = maxYear;
  yearRangeMax.value = activeEndYear || maxYear;

  startYearLabel.textContent = activeStartYear || minYear;
  endYearLabel.textContent = activeEndYear || maxYear;

  // Initialize custom track colors on page load
  updateYearSliderTrack(yearRangeMin, yearRangeMax, yearSliderTrack, minYear, maxYear);
}

function initializeListFilters() {
  // Get all available lists from listsMap (core + custom)
  const availableLists = Object.entries(listsMap)
    .filter(([, config]) =>
      // Only include lists for the current user or default lists
      config.username === userSettings.username || config.username === 'default'
    )
    .filter(([, config]) =>
      // Only include lists that should be shown in filters
      config.type === 'core' || config.showInFilters
    )
    .map(([listName, config]) => ({
      id: `${listName}-filter-icon`,
      listName,
      type: config.type || 'core'
    }));

  // Add default core lists if not present
  const coreLists = ['favourites', 'watchlist', 'collection', 'watched'];
  coreLists.forEach(listName => {
    if (!availableLists.some(list => list.listName === listName)) {
      availableLists.push({
        id: `${listName}-filter-icon`,
        listName,
        type: 'core'
      });
    }
  });

  // Add event listeners to each filter button
  availableLists.forEach(({ id, listName, type }) => {
    const button = document.getElementById(id);

    if (!button) {
      console.error(`Button with ID ${id} not found.`);
      return;
    }

    // Set initial state based on activeLists
    if (activeLists.some(list => list.name === listName && list.type === type)) {
      button.classList.add("active");
    } else {
      button.classList.remove("active");
    }
  });
}

// Global resetGenreFilters function
function resetGenreFilters() {
  if (activeGenres.length === 0) {
    console.log("No active genres to reset");
    return;
  }

  activeGenres = [];

  const pills = document.querySelectorAll(".genre-pill.selected");
  for (let i = 0; i < pills.length; i++) {
    pills[i].classList.remove("selected");
  }

  //applyFiltersAndSearch();
  debouncedApplyFilters();
  updateFilterButtonState(document.getElementById("genre-filter-button"));
}

// Handler for genre button click
function handleGenreButtonClick() {
  // Only reset genre filters if any are active
  if (activeGenres.length > 0) {
    resetGenreFilters();
  }
  // Do nothing if no genres are active
}

// Modified initializeGenreFilter function without the resetGenreFilters function
function initializeGenreFilter() {
  const genreContainer = document.getElementById("genre-container");
  const genreFilterButton = document.getElementById("genre-filter-button");

  // Ensure genreCache is populated
  if (!genreCache || genreCache.length === 0) {
    console.error("No genres available to initialize the genre filter.");
    return;
  }

  // Clear any existing content in the container
  genreContainer.innerHTML = "";

  // Convert movieCache to an array for filtering
  const moviesArray = Array.from(mediaCache.values());

  genreCache.forEach((genre) => {
    const genrePill = document.createElement("div");
    genrePill.classList.add("genre-pill");
    genrePill.textContent = genre.name;
    genrePill.dataset.id = genre.genreID;

    // Check if any movie in the cache has this genre
    const genreID = parseInt(genre.genreID, 10);
    const hasMovies = moviesArray.some((movie) => {
      return Array.isArray(movie.genre_ids) &&
        movie.genre_ids.some(id => parseInt(id, 10) === genreID);
    });

    if (!hasMovies) {
      // Disable the pill
      genrePill.classList.add("disabled");
      genrePill.style.opacity = 0.5;
      genrePill.style.pointerEvents = "none";
    } else {
      // If movies exist for this genre, set its initial selected state
      if (activeGenres.includes(genre.genreID)) {
        genrePill.classList.add("selected");
      }

      // Toggle selection on click
      genrePill.addEventListener("click", () => {
        if (genrePill.classList.contains("selected")) {
          // Deselect the pill
          genrePill.classList.remove("selected");
          activeGenres = activeGenres.filter((id) => id !== genreID);
        } else {
          // Select the pill
          genrePill.classList.add("selected");
          activeGenres.push(genreID);
        }
        // Apply all filters after updating selected genres
        applyFiltersAndSearch();
        updateFilterButtonState(genreFilterButton);
      });
    }

    // Append the pill to the container
    genreContainer.appendChild(genrePill);
  });

  // Update button state on initial load
  updateFilterButtonState(genreFilterButton);

}

/**
 * Updates the background color of a slider track based on the selected range.
 */
function updateRatingSliderTrack(rangeStart, rangeEnd, track, min, max) {
  const [startValue, endValue] = [parseFloat(rangeStart.value), parseFloat(rangeEnd.value)];

  // Calculate percentages for start and end values relative to min and max
  const [startPercentage, endPercentage] = [
    ((startValue - min) / (max - min)) * 100,
    ((endValue - min) / (max - min)) * 100,
  ];

  // Update the background gradient of the custom track
  track.style.background = `linear-gradient(to right,
    #3d3d3d ${startPercentage}%,
    #5d5d5d ${startPercentage}%,
    #5d5d5d ${endPercentage}%,
    #3d3d3d ${endPercentage}%)`;
}

/**
 * Updates the position and value of a slider thumb's value element.
 */
function updateRatingThumbValue(range, valueSpan, appendPercentage = false) {
  const rangeRect = range.getBoundingClientRect();
  const thumbWidth = 30; // Match the thumb width in CSS

  // Calculate position based on current value
  const position = ((range.value - range.min) / (range.max - range.min)) * (rangeRect.width - thumbWidth);

  // Update displayed value and position
  const displayValue = appendPercentage ? `${range.value}%` : range.value;
  valueSpan.textContent = displayValue;
  valueSpan.style.left = `${position + thumbWidth / 2}px`;

  // Update active state based on whether thumb is at min/max position
  const isAtDefault = (range.id.includes("start") && range.value === range.min) ||
    (range.id.includes("end") && range.value === range.max);

  if (isAtDefault) {
    range.classList.remove("active");
  } else {
    range.classList.add("active");
  }
}

/**
 * Updates all slider thumb values and their positions, and updates slider tracks.
 */
function updateAllRatingValues(sliders) {
  Object.values(sliders).forEach(({ rangeStart, rangeEnd, startValue, endValue, track, min, max }) => {
    // Update thumb positions
    updateRatingThumbValue(rangeStart, startValue);
    updateRatingThumbValue(rangeEnd, endValue);

    // Update slider track colors
    updateRatingSliderTrack(rangeStart, rangeEnd, track, parseFloat(min), parseFloat(max));
  });
}

/**
 * Handles slider updates by updating global filter variables,
 * positioning thumbs, resetting filters when sliders are at default positions,
 * and enforcing min/max constraints.
 */
function handleRatingSliderUpdate(rangeStart, rangeEnd, startSpan, endSpan, callback, defaultMin, defaultMax, track = null, min = 0, max = 100) {
  let startValue = parseFloat(rangeStart.value);
  let endValue = parseFloat(rangeEnd.value);

  // Prevent rangeStart from going beyond rangeEnd
  if (startValue > endValue) {
    if (rangeStart === document.activeElement) {
      endValue = startValue;
      rangeEnd.value = endValue;
    } else {
      startValue = endValue;
      rangeStart.value = startValue;
    }
  }

  // Update thumb positions
  updateRatingThumbValue(rangeStart, startSpan);
  updateRatingThumbValue(rangeEnd, endSpan);

  // Update slider track colors
  if (track) {
    updateRatingSliderTrack(rangeStart, rangeEnd, track, parseFloat(min), parseFloat(max));
  }

  // Update active state for thumbs
  if (startValue === parseFloat(defaultMin)) {
    rangeStart.classList.remove("active");
  } else {
    rangeStart.classList.add("active");
  }

  if (endValue === parseFloat(defaultMax)) {
    rangeEnd.classList.remove("active");
  } else {
    rangeEnd.classList.add("active");
  }

  // Update global filter variables
  if (startValue === parseFloat(defaultMin) && endValue === parseFloat(defaultMax)) {
    callback(null, null); // Reset global filter variables
  } else {
    callback(startValue, endValue); // Update global filter variables
  }

  // Update button state dynamically
  updateFilterButtonState(document.getElementById("ratings-filter-button"));
}

/**
 * Initialize the ratings filter sliders and set up event listeners.
 */
function initializeRatingsFilter() {
  if (ratingsInitialized) return null; // Prevent multiple initializations

  ratingsInitialized = true;
  const sliders = {
    imdb: {
      rangeStart: document.getElementById("imdb-range-start"),
      rangeEnd: document.getElementById("imdb-range-end"),
      startValue: document.getElementById("imdb-range-start-value"),
      endValue: document.getElementById("imdb-range-end-value"),
      track: document.getElementById("imdb-slider-track"),
      min: "0",
      max: "10",
    },
    tmdb: {
      rangeStart: document.getElementById("tmdb-range-start"),
      rangeEnd: document.getElementById("tmdb-range-end"),
      startValue: document.getElementById("tmdb-range-start-value"),
      endValue: document.getElementById("tmdb-range-end-value"),
      track: document.getElementById("tmdb-slider-track"),
      min: "0",
      max: "10",
    },
    metascore: {
      rangeStart: document.getElementById("metascore-range-start"),
      rangeEnd: document.getElementById("metascore-range-end"),
      startValue: document.getElementById("metascore-range-start-value"),
      endValue: document.getElementById("metascore-range-end-value"),
      track: document.getElementById("metascore-slider-track"),
      min: "0",
      max: "100",
    },
    rt: {
      rangeStart: document.getElementById("rt-range-start"),
      rangeEnd: document.getElementById("rt-range-end"),
      startValue: document.getElementById("rt-range-start-value"),
      endValue: document.getElementById("rt-range-end-value"),
      track: document.getElementById("rt-slider-track"),
      min: "0",
      max: "100",
    },
  };

  /**
   * Attach event listeners to sliders dynamically.
   */
  Object.entries(sliders).forEach(([key, { rangeStart, rangeEnd, startValue, endValue, track, min, max }]) => {
    const callback = (start, end) => {
      switch (key) {
        case "imdb":
          activeMinImdb = start;
          activeMaxImdb = end;
          break;
        case "tmdb":
          activeMinTmdb = start;
          activeMaxTmdb = end;
          break;
        case "metascore":
          activeMinMetascore = start;
          activeMaxMetascore = end;
          break;
        case "rt":
          activeMinRT = start;
          activeMaxRT = end;
          break;
        default:
          break;
      }
    };

    const handleInputEvent = () => {
      handleRatingSliderUpdate(
        rangeStart,
        rangeEnd,
        startValue,
        endValue,
        callback,
        min.toString(),
        max.toString(),
        track,
        min,
        max
      );
      debouncedApplyFilters();
    };

    // Add event listeners for both sliders
    rangeStart.addEventListener("input", handleInputEvent);
    rangeEnd.addEventListener("input", handleInputEvent);

    // Set initial thumb values and tracks
    updateRatingThumbValue(rangeStart, startValue);
    updateRatingThumbValue(rangeEnd, endValue);
    updateRatingSliderTrack(rangeStart, rangeEnd, track, parseFloat(min), parseFloat(max));
  });

  // Add click event listener to reset ratings filters when clicking on the button
  ratingsFilterButton.addEventListener("click", () => toggleRatingSort('ratings-filter'));

  // Ensure proper alignment after DOM content is fully loaded
  window.addEventListener("load", () => updateAllRatingValues(sliders));
  return sliders;
}

async function performSearch(query, category, signal) {
  if (!query || query.trim() === "") return [];

  // Check for abort before starting
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");

  try {
    if (category === "All") {
      return await searchAllStores(query, signal);
    } else if (category === "People") {
      const peopleMatches = await queryIndexedDB(db, "people", ["name"], query, normalizeForName);
      if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
      await yieldToUI();
      return await searchItemsWithLookup(peopleMatches, query, ["name"], db, searchMapping.People.lookupFlow, signal);
    } else if (category === "Keywords") {
      const keywordMatches = await queryIndexedDB(db, "keywords", ["name"], query, normalizeForName);
      if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
      await yieldToUI();
      return await searchItemsWithLookup(keywordMatches, query, ["name"], db, searchMapping.Keywords.lookupFlow, signal);
    } else if (searchMapping[category]) {
      const mapping = searchMapping[category];
      return await chunkedSearch(mapping.items(), query, mapping.fields, normalizeForSearch, signal);
    } else {
      console.error("Unknown search category:", category);
      return [];
    }
  } catch (error) {
    if (error.name === 'AbortError') {
      throw error; // Re-throw abort errors
    }
    console.error(`Error in ${category} search:`, error);
    return [];
  }
}

/**
 * UI/DB: global search in indexedDB
 * @param {*} items 
 * @param {*} searchQuery 
 * @param {*} fields 
 * @param {*} normFn 
 * @returns 
 */
function searchItems(items, searchQuery, fields, normFn = normalizeForSearch) {
  if (!items || !Array.isArray(items)) {
    console.error("Invalid input: 'items' must be an array.");
    return [];
  }

  // Properly handle empty or whitespace-only queries
  if (!searchQuery || typeof searchQuery !== "string" || searchQuery.trim() === "") {
    console.warn("Search query is empty or invalid.");
    return sortItems(items);
  }

  // Normalize the trimmed query
  const searchNormalized = normFn(searchQuery.trim());

  // Arrays for different match types AND different fields
  const exactMatchesTitle = [];
  const startsWithMatchesTitle = [];
  const containsMatchesTitle = [];
  const exactMatchesOther = [];
  const startsWithMatchesOther = [];
  const containsMatchesOther = [];

  items.forEach(item => {
    // Check primary title field first (index 0)
    const primaryField = fields[0];
    const primaryValue = normFn(item[primaryField] || "");
    const sortKey = primaryValue;

    // Check if match is in primary title field
    if (primaryValue === searchNormalized) {
      exactMatchesTitle.push({ ...item, sortKey, _matchField: primaryField });
    } else if (primaryValue.startsWith(searchNormalized)) {
      startsWithMatchesTitle.push({ ...item, sortKey, _matchField: primaryField });
    } else if (primaryValue.includes(searchNormalized)) {
      containsMatchesTitle.push({ ...item, sortKey, _matchField: primaryField });
    } else {
      // Check other fields
      const otherFields = fields.slice(1);
      const normalizedOtherFields = otherFields.map(field => normFn(item[field] || ""));

      if (normalizedOtherFields.some(field => field === searchNormalized)) {
        exactMatchesOther.push({ ...item, sortKey, _matchField: "other" });
      } else if (normalizedOtherFields.some(field => field.startsWith(searchNormalized))) {
        startsWithMatchesOther.push({ ...item, sortKey, _matchField: "other" });
      } else if (normalizedOtherFields.some(field => field.includes(searchNormalized))) {
        containsMatchesOther.push({ ...item, sortKey, _matchField: "other" });
      }
    }
  });

  // Return results in priority order: primary field matches first, then other fields
  return [
    ...sortItems(exactMatchesTitle),
    ...sortItems(startsWithMatchesTitle),
    ...sortItems(containsMatchesTitle),
    ...sortItems(exactMatchesOther),
    ...sortItems(startsWithMatchesOther),
    ...sortItems(containsMatchesOther)
  ];
}

/**
 * Helper: Sort strings
 * @param {*} items 
 * @returns 
 */
function sortItems(items) {
  return items.sort((a, b) => a.sortKey.localeCompare(b.sortKey));
}

/**
 * DB: lookup values in DB
 * @param {*} db 
 * @param {*} storeName 
 * @param {*} fields 
 * @param {*} value 
 * @param {*} normFn 
 * @returns 
 */
async function queryIndexedDB(db, storeName, fields, value, normFn = normalizeForSearch) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([storeName], "readonly");
    const store = transaction.objectStore(storeName);

    // Handle people store name searches with partial matching
    if (storeName === "people" && fields[0] === "name") {
      const request = store.getAll();
      request.onsuccess = () => {
        const items = request.result || [];
        const normalizedValue = normFn(String(value));
        const matches = items.filter(item =>
          normFn(String(item[fields[0]] || "")).includes(normalizedValue)
        );
        resolve(matches);
      };
      request.onerror = () => {
        console.error(`Error querying store "${storeName}":`, request.error);
        reject(request.error);
      };
    }
    // Handle ID fields with exact matching
    else if (fields[0] === "personID" || fields[0] === "keywordID") {
      try {
        const index = store.index(fields[0]);
        const request = index.getAll(value);
        request.onsuccess = () => resolve(request.result || []);
        request.onerror = () => {
          console.error(`Error querying index "${fields[0]}" in store "${storeName}":`, request.error);
          reject(request.error);
        };
      } catch {
        // Fallback to manual exact matching
        const request = store.getAll();
        request.onsuccess = () => {
          const items = request.result || [];
          // Convert both values to strings with null checks
          const stringValue = String(value ?? '');
          const matches = items.filter(item =>
            String(item[fields[0]] ?? '') === stringValue
          );
          resolve(matches);
        };
        request.onerror = () => {
          console.error(`Error querying store "${storeName}":`, request.error);
          reject(request.error);
        };
      }
    }
    // Handle other stores with partial matching
    else {
      const request = store.getAll();
      request.onsuccess = () => {
        const items = request.result || [];
        const normalizedValue = normFn(String(value));
        const matches = items.filter(item =>
          fields.some(field =>
            normFn(String(item[field] || "")).includes(normalizedValue)
          )
        );
        resolve(matches);
      };
      request.onerror = () => {
        console.error(`Error querying store "${storeName}":`, request.error);
        reject(request.error);
      };
    }
  });
}

/**
 * DB: search across all stores with chunked processing
 * @param {*} query 
 * @returns 
 */
async function searchAllStores(query, signal) {
  if (!query || query.trim() === "") return [];
  query = query.trim();

  showNotification('Searching...', true);

  try {
    // Search Movies using searchMapping
    const movieResults = await chunkedSearch(
      searchMapping.Movies.items(),
      query,
      searchMapping.Movies.fields, 
      normalizeForSearch,
      signal
    );

    if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
    await yieldToUI();

    // Search Series using searchMapping
    const seriesResults = await chunkedSearch(
      searchMapping.Series.items(), 
      query,
      searchMapping.Series.fields,  
      normalizeForSearch,
      signal
    );

    // Deduplicate and combine movie and series results
    const seenIds = new Set();
    const initialResults = [];
    
    // Add unique movie results
    for (const movie of movieResults) {
      const id = movie.imdbID || movie.id;
      if (id && !seenIds.has(id)) {
        seenIds.add(id);
        initialResults.push(movie);
      }
    }
    
    // Add unique series results
    for (const series of seriesResults) {
      const id = series.imdbID || series.id;
      if (id && !seenIds.has(id)) {
        seenIds.add(id);
        initialResults.push(series);
      }
    }

    applyFiltersAndSearch(false, initialResults, true);
    updateMatchCount();

    if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
    await yieldToUI();

    // Search People using searchMapping
    const peopleMatches = await queryIndexedDB(
      db,
      "people",
      searchMapping.People.fields, 
      query,
      normalizeForName
    );

    const peopleResults = await searchItemsWithLookup(
      peopleMatches,
      query,
      searchMapping.People.fields, 
      db,
      searchMapping.People.lookupFlow,
      signal
    );

    // Search Keywords using searchMapping
    const keywordMatches = await queryIndexedDB(
      db,
      "keywords",
      searchMapping.Keywords.fields, 
      query,
      normalizeForName
    );

    const keywordResults = await searchItemsWithLookup(
      keywordMatches,
      query,
      searchMapping.Keywords.fields, 
      db,
      searchMapping.Keywords.lookupFlow,
      signal
    );

    // Add unique people and keyword results
    const allResults = [...initialResults];
    
    // Add unique people results
    for (const person of peopleResults) {
      const id = person.imdbID || person.id;
      if (id && !seenIds.has(id)) {
        seenIds.add(id);
        allResults.push(person);
      }
    }
    
    // Add unique keyword results
    for (const keyword of keywordResults) {
      const id = keyword.imdbID || keyword.id;
      if (id && !seenIds.has(id)) {
        seenIds.add(id);
        allResults.push(keyword);
      }
    }

    if (peopleResults.length > 0 || keywordResults.length > 0) {
      applyFiltersAndSearch(false, allResults, false);
      updateMatchCount();
    }

    return allResults;
  } finally {
    removeNotification(note => note.isProgress && !note.isBackgroundTask);
  }
}

/**
 * Yield to the UI thread to prevent blocking
 */
function yieldToUI() {
  return new Promise(resolve => requestAnimationFrame(resolve));
}

/**
 * Chunked search implementation to prevent UI blocking
 */
async function chunkedSearch(items, searchQuery, fields, normFn = normalizeForSearch, signal) {
  if (!items || !Array.isArray(items)) {
    console.error("Invalid input: 'items' must be an array.");
    return [];
  }

  // Properly handle empty or whitespace-only queries
  if (!searchQuery || typeof searchQuery !== "string" || searchQuery.trim() === "") {
    console.warn("Search query is empty or invalid.");
    return [];
  }

  searchQuery = searchQuery.trim();
  const searchNormalized = normFn(searchQuery);

  // Separate arrays for primary field (Title) matches and secondary field matches
  const exactMatchesPrimary = [];
  const startsWithMatchesPrimary = [];
  const containsMatchesPrimary = [];
  const exactMatchesSecondary = [];
  const startsWithMatchesSecondary = [];
  const containsMatchesSecondary = [];

  // Process in chunks of 500 items
  const chunkSize = 500;
  const chunks = Math.ceil(items.length / chunkSize);

  for (let i = 0; i < chunks; i++) {
    // Check for abort signal
    if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");

    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, items.length);
    const chunk = items.slice(start, end);

    // Process this chunk
    for (const item of chunk) {
      // Check primary field first (Title)
      const primaryField = fields[0];
      const primaryValue = item[primaryField] ? normFn(String(item[primaryField])) : "";
      const sortKey = primaryValue || ""; // Use primary field as sort key

      // Check if match is in primary field
      if (primaryValue === searchNormalized) {
        exactMatchesPrimary.push({ ...item, sortKey, _fieldMatchType: 1 });
      } else if (primaryValue.startsWith(searchNormalized)) {
        startsWithMatchesPrimary.push({ ...item, sortKey, _fieldMatchType: 2 });
      } else if (primaryValue.includes(searchNormalized)) {
        containsMatchesPrimary.push({ ...item, sortKey, _fieldMatchType: 3 });
      } else {
        // Check secondary fields if not found in primary
        const secondaryFields = fields.slice(1);
        const secondaryValues = secondaryFields.map(field => {
          const value = item[field];
          return value ? normFn(String(value)) : "";
        });

        if (secondaryValues.some(value => value === searchNormalized)) {
          exactMatchesSecondary.push({ ...item, sortKey, _fieldMatchType: 4 });
        } else if (secondaryValues.some(value => value.startsWith(searchNormalized))) {
          startsWithMatchesSecondary.push({ ...item, sortKey, _fieldMatchType: 5 });
        } else if (secondaryValues.some(value => value.includes(searchNormalized))) {
          containsMatchesSecondary.push({ ...item, sortKey, _fieldMatchType: 6 });
        }
      }
    }

    // Yield to UI after each chunk
    if (i < chunks - 1) {
      await yieldToUI();
    }
  }

  // Sort results in chunks to avoid long-running sorts
  const sortedExactPrimary = await chunkedSort(exactMatchesPrimary, signal);
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
  await yieldToUI();

  const sortedStartsWithPrimary = await chunkedSort(startsWithMatchesPrimary, signal);
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
  await yieldToUI();

  const sortedContainsPrimary = await chunkedSort(containsMatchesPrimary, signal);
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
  await yieldToUI();

  const sortedExactSecondary = await chunkedSort(exactMatchesSecondary, signal);
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
  await yieldToUI();

  const sortedStartsWithSecondary = await chunkedSort(startsWithMatchesSecondary, signal);
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");
  await yieldToUI();

  const sortedContainsSecondary = await chunkedSort(containsMatchesSecondary, signal);

  // Return results in priority order: primary field matches first, then secondary field matches
  return [
    ...sortedExactPrimary,
    ...sortedStartsWithPrimary,
    ...sortedContainsPrimary,
    ...sortedExactSecondary,
    ...sortedStartsWithSecondary,
    ...sortedContainsSecondary
  ];
}

/**
 * Sort items in chunks to prevent UI blocking
 */
async function chunkedSort(items, signal) {
  if (items.length <= 1) return items;

  // Check for abort signal
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");

  // For small arrays, just sort directly
  if (items.length < 1000) {
    return items.sort((a, b) => a.sortKey.localeCompare(b.sortKey));
  }

  // For larger arrays, use merge sort with chunking
  const chunkSize = 500;
  const chunks = [];

  // Split into chunks and sort each chunk
  for (let i = 0; i < items.length; i += chunkSize) {
    // Check for abort signal
    if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");

    const chunk = items.slice(i, i + chunkSize);
    chunks.push(chunk.sort((a, b) => a.sortKey.localeCompare(b.sortKey)));

    // Yield to UI after sorting each chunk
    if (i + chunkSize < items.length) {
      await yieldToUI();
    }
  }

  // Merge sorted chunks
  return mergeSortedArrays(chunks);
}

/**
 * Merge multiple sorted arrays into one sorted array
 */
function mergeSortedArrays(arrays) {
  // If only one array, return it
  if (arrays.length === 1) return arrays[0];

  // Create a result array
  const result = [];

  // Create an array of indices, one for each input array
  const indices = new Array(arrays.length).fill(0);

  // While we haven't exhausted all arrays
  while (true) {
    let minValue = null;
    let minIndex = -1;

    // Find the smallest value among the heads of all arrays
    for (let i = 0; i < arrays.length; i++) {
      if (indices[i] < arrays[i].length) {
        const value = arrays[i][indices[i]];
        if (minValue === null || value.sortKey.localeCompare(minValue.sortKey) < 0) {
          minValue = value;
          minIndex = i;
        }
      }
    }

    // If we didn't find a minimum value, we're done
    if (minIndex === -1) break;

    // Add the minimum value to the result and advance the index
    result.push(minValue);
    indices[minIndex]++;
  }

  return result;
}

/**
 * Optimized searchItemsWithLookup with chunked processing
 */
async function searchItemsWithLookup(items, searchQuery, fields, db, lookupFlow = null, signal) {
  if (!items || !Array.isArray(items)) {
    console.error("Invalid input: 'items' must be an array.");
    return [];
  }

  // Properly handle empty or whitespace-only queries
  if (!searchQuery || typeof searchQuery !== "string" || searchQuery.trim() === "") {
    console.warn("Search query is empty or invalid.");
    return [];
  }

  searchQuery = searchQuery.trim();

  // Normalize the search query
  const normFn = (lookupFlow && lookupFlow.type === "people") ? normalizeForName : normalizeForSearch;
  const searchNormalized = normFn(searchQuery);

  // Filter items based on the normalized search query and fields
  const matchedItems = items.filter(item =>
    fields.some(field => normFn(String(item[field] || "")).includes(searchNormalized))
  );

  if (!lookupFlow) {
    return matchedItems;
  }

  // Check for abort signal
  if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");

  // Extract IDs from the matched items based on the lookupFlow type
  const ids = matchedItems.map(item => item[lookupFlow.idField])
    .filter(id => id !== undefined && id !== null);

  const tmdbIDs = new Set();
  // Handle multiple store names if provided, otherwise use single storeName
  const storeNames = lookupFlow.storeNames || [lookupFlow.storeName];

  // Process IDs in chunks
  const idChunkSize = 20;
  for (let i = 0; i < ids.length; i += idChunkSize) {
    // Check for abort signal
    if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");

    const idChunk = ids.slice(i, i + idChunkSize);

    // Process this chunk of IDs
    for (const id of idChunk) {
      for (const storeName of storeNames) {
        const credits = await queryIndexedDB(db, storeName, [lookupFlow.idField], id);
        credits.forEach(credit => {
          if (credit.tmdbID) tmdbIDs.add(credit.tmdbID);
        });
      }
    }

    // Yield to UI after each chunk
    if (i + idChunkSize < ids.length) {
      await yieldToUI();
    }
  }

  // Map the tmdbIDs to media from mediaCache in chunks
  const tmdbIDArray = Array.from(tmdbIDs);
  const results = [];
  const mediaArray = Array.from(mediaCache.values());

  const idMapChunkSize = 100;
  for (let i = 0; i < tmdbIDArray.length; i += idMapChunkSize) {
    // Check for abort signal
    if (signal?.aborted) throw new DOMException("Search aborted", "AbortError");

    const idMapChunk = tmdbIDArray.slice(i, i + idMapChunkSize);

    for (const tmdbID of idMapChunk) {
      const media = mediaArray.find(m => m.tmdbID === tmdbID);
      if (media) results.push(media);
    }

    // Yield to UI after each chunk
    if (i + idMapChunkSize < tmdbIDArray.length) {
      await yieldToUI();
    }
  }

  return results;
}

/**
 * UI: Helper template for list icons
 * @param {*} imdbID 
 * @returns 
 */
function createTopRowIcons(imdbID) {
  if (!iconFragment) {
    console.error('Icon template not initialized');
    return document.createElement('div');
  }

  const clone = iconFragment.cloneNode(true);
  updateIconStates(clone, imdbID);
  return clone;
}

async function handleIconClick(_event, element) {
  const icon = element;
  const container = icon.closest('[data-imdbid]');
  const imdbID = container ? container.getAttribute('data-imdbid') : null;

  if (!imdbID) {
    console.error('Icon not within a valid container with IMDb ID:', icon);
    return;
  }

  // Ensure we have a user
  if (!userSettings?.username) {
    console.error('No active user. Cannot update lists.');
    showNotification('❌ Please log in to update lists', false);
    return;
  }

  const listType = icon.dataset.listType;
  const config = getListConfig(listType);
  const wasActive = icon.dataset.state === 'active';
  const tooltipElement = icon.querySelector('.ttip-txt');

  try {
    // Optimistically update the UI
    icon.dataset.state = wasActive ? 'inactive' : 'active';
    const svgElement = icon.querySelector('.icon-svg');
    if (svgElement) {
      svgElement.style.color = wasActive ? 'var(--default-color)' : `var(--${listType}-color)`;
    }

    if (tooltipElement) {
      tooltipElement.textContent = wasActive ? `Add to ${config.label}` : `Remove from ${config.label}`;
    }

    // Perform the async database operation with username
    await toggleListMembership(imdbID, listType, userSettings.username);

    // Update cache
    const movie = mediaCache.get(imdbID);
    if (movie) {
      if (!movie.lists) movie.lists = {};
      movie.lists[listType] = !wasActive;
      mediaCache.set(imdbID, { ...movie });
    }

    // Update any duplicate icons
    document.querySelectorAll(`[data-imdbid="${imdbID}"] .top-row-icons`)
      .forEach(containerElement => {
        updateIconStates(containerElement, imdbID);
      });

  } catch (error) {
    console.error('List toggle failed:', error);
    showNotification('❌ Failed to update list. Please try again.', false);

    // Revert UI on error
    icon.dataset.state = wasActive ? 'active' : 'inactive';
    const svgElement = icon.querySelector('.icon-svg');
    if (svgElement) {
      svgElement.style.color = wasActive ? `var(--${listType}-color)` : 'var(--default-color)';
    }

    if (tooltipElement) {
      tooltipElement.textContent = wasActive ? `Remove from ${config.label}` : `Add to ${config.label}`;
    }
  }
}

/**
 * UI: Create placeholder SVGs if no image is available ### refactor (profile part not used anymore)
 * @param {*} width 
 * @param {*} height 
 * @param {*} type 
 * @returns 
 */
function createPlaceholderSVG(width, height, type = 'profile') {
  if (type === 'poster') {
    return `<svg width="${width}" height="${height}" class="placeholder-poster">
      <use href="#filmy_logo" class="low-opacity" />
    </svg>`;
  } else {
    return `<svg width="${width}" height="${height}" class="placeholder-profile">
      <use href="#placeholder_profile" class="low-opacity" />
    </svg>`;
  }
}

async function initializeIcons() {
  const container = document.createElement('div');
  // Get the preferred display order 
  const filterOrder = ['favourites', 'watched', 'watchlist', 'collection'];

  // Only include lists that exist in listsMap or fallback to defaults
  const iconHTML = filterOrder.map(listName => {
    const config = getListConfig(listName);
    return `
      <div class="icon-container ${listName}" data-list-type="${listName}" data-tooltip>
        <svg width="20" height="20" class="icon-svg">
          <use href="#${config.symbolId}" />
        </svg>
        <div class="ttip-txt" data-tt-pos="left">
          ${config.label}
        </div>
      </div>`;
  }).join('');

  container.innerHTML = `<div class="top-row-icons">${iconHTML}</div>`;
  iconFragment = container.firstElementChild;
}

function updateIconStates(iconsContainer, imdbID) {
  const mediaData = mediaCache.get(imdbID);

  // Error handling with helpful message
  if (!mediaData || !mediaData.lists) {
    console.error('Movie data or lists unavailable for', imdbID);
    return;
  }

  iconsContainer.querySelectorAll(".icon-container").forEach(icon => {
    const listType = icon.dataset.listType;
    if (!listType) return; // Skip if no list type defined

    const config = getListConfig(listType);
    const isActive = mediaData.lists[listType] === true;

    // Set the state attribute
    icon.dataset.state = isActive ? "active" : "inactive";

    // Update icon color
    const svgElement = icon.querySelector('.icon-svg');
    if (svgElement) {
      svgElement.style.color = isActive ?
        `var(--${listType}-color)` : 'var(--default-color)';
    }

    // Update tooltip text
    const btnTip = icon.querySelector('.ttip-txt');
    if (btnTip) {
      btnTip.textContent = isActive
        ? `Remove from ${config.label}`
        : `Add to ${config.label}`;
    }
  });
}

/**
 * UI: Template for displaying Ratings
 * @param {*} mediaData 
 * @returns 
 */
function generateRatingsHtml(mediaData) {

  // Map of special characters (superscripts and fractions) to their normal equivalents
  const specialCharacterMap = {
    '⁰': ' 0',
    '¹': ' 1',
    '²': ' 2',
    '³': ' 3',
    '⁴': ' 4',
    '⁵': ' 5',
    '⁶': ' 6',
    '⁷': ' 7',
    '⁸': ' 8',
    '⁹': ' 9',
    '½': ' 1/2' // Replace fraction ½ with 1/2
  };

  // Normalize the title for exception key matching
  const normalizedTitle = mediaData.Title
    .normalize('NFD')
    .replace(/[⁰¹²³⁴⁵⁶⁷⁸⁹½]/g, match => specialCharacterMap[match])
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^\w\d\s-]/g, '')
    .trim()
    .replace(/\s+/g, '_')
    .replace(/-/g, '_')
    .toLowerCase();

  // Create the normalized exception key
  const exceptionKey = `${normalizedTitle}_${mediaData.Year}`;

  const formattedVoteCount = parseFloat(mediaData.vote_count)
    .toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 });

  // Build IMDb rating
  let imdbRatingLink = "";
  if (!mediaData.imdbID.startsWith("tmdb-")) {
    const imdbRatingObj = mediaData.Ratings && mediaData.Ratings.find(rating => rating.Source === "Internet Movie Database");
    const imdbRating = imdbRatingObj ? imdbRatingObj.Value.split('/')[0] : null;
    imdbRatingLink = imdbRating
      ? `<a href="https://www.imdb.com/title/${mediaData.imdbID}" class="rating-link" target="_blank"><span class="imdb-rating">${imdbRating}</span><span class="imdb-votes">${mediaData.imdbVotes}</span><span class="ttip-txt" data-tt-pos="bottom">🔗 IMDb</span></a>`
      : `<a href="https://www.imdb.com/title/${mediaData.imdbID}" target="_blank" class="rating-link"><span class="imdb-rating">NR</span><span class="imdb-votes">–</span><span class="ttip-txt" data-tt-pos="bottom">🔗 IMDb</span></a>`;
  }

  // Build TMDB rating
  let tmdbRatingLink = "";
  if (mediaData.tmdbID && mediaData.vote_average) {
    const tmdbVotes = parseFloat(mediaData.vote_average).toFixed(1);
    tmdbRatingLink = `<a href="${tmdbBaseUrl}${mediaData.mediaType}/${mediaData.tmdbID}" target="_blank" class="rating-link"><span class="tmdb-rating">${tmdbVotes}</span><span class="tmdb-votes">${formattedVoteCount}</span><span class="ttip-txt" data-tt-pos="bottom">🔗 TMDB</span></a>`;
  } else {
    tmdbRatingLink = `<a href="${tmdbBaseUrl}${mediaData.mediaType}/${mediaData.tmdbID}" target="_blank" class="rating-link"><span class="tmdb-rating">NR</span><span class="tmdb-votes">–</span><span class="ttip-txt" data-tt-pos="bottom">🔗 TMDB</span></a>`;
  }

  const metacriticUrlExceptions = {
    // Example keys: normalizedTitle_Year
    "aftermath_2017": "aftermath_2017",
  };

  const baseMetacriticUrl = "https://www.metacritic.com/movie/";

  // Generate Metascore icon/link
  const msUrl = metacriticUrlExceptions[exceptionKey]
    ? baseMetacriticUrl + metacriticUrlExceptions[exceptionKey]
    : `${baseMetacriticUrl}${normalizedTitle.replace(/_/g, '-')}`;
    
  const metascore = mediaData.Metascore && mediaData.Metascore !== "N/A" ? parseInt(mediaData.Metascore) : null;
  let metascoreIcon = "";
  if (metascore !== null) {
    let colorClass;
    if (metascore > 60) {
      colorClass = "meta60";
    } else if (metascore >= 40) {
      colorClass = "meta40";
    } else {
      colorClass = "metared";
    }

    metascoreIcon = `
    <a href="${msUrl}" target="_blank" class="rating-link ${colorClass}">
      <span class="rating-icon meta">
        <svg class="metascore-icon">
          <use href="#metascore"/>
        </svg>
        <span class="meta-rating">${metascore}</span>
      </span>
      <span class="ttip-txt" data-tt-pos="bottom">🔗 Metacritic</span>
    </a>
  `;
  }

  // Prepare Rotten Tomatoes rating, define exceptions for links that don't follow the pattern
  const rtUrlExceptions = {
    "9_2009": "1205483_nine",
    "1408_2007": "1408",
    "1911_2011": "1911",
    "8_2020": "the_soul_collector_2020",
    "21_2008": "10009192-21",
    "2010_1984": "2010_the_year_we_make_contact",
    "2012_2009": "2012",
    "aftermath_2017": "aftermath_2017",
    "the_aftermath_2019": "the_aftermath_2019",
    "dr_strangelove_or_how_i_learned_to_stop_worrying_and_love_the_bomb_1964": "dr_strangelove",
    "friday_the_13th_part_vi_jason_lives_1986": "friday_the_13th_part_6_jason_lives",
    "friday_the_13th_the_new_blood_1988": "friday_the_13th_part_7_the_new_blood",
    "friday_the_13th_part_viii_jason_takes_manhattan_1989": "friday_the_13th_part_8_jason_takes_manhattan",
    "friday_the_13th_2009": "friday_the_13th_prequel",
    "friday_the_13th_1980": "friday_the_13th_part_1",
    "fright_night_1985": "1007910-fright_night",
    "fright_night_2011": "fright_night_2011",
  };

  const baseRtUrl = "https://www.rottentomatoes.com/m/";
  let rtUrl;
  if (rtUrlExceptions[exceptionKey]) {
    // Always prepend the base URL to the relative path
    rtUrl = baseRtUrl + rtUrlExceptions[exceptionKey];
  } else {
    if (/^\d+$/.test(normalizedTitle)) {
      rtUrl = `${baseRtUrl}${normalizedTitle}_${mediaData.Year}`;
    } else {
      rtUrl = `${baseRtUrl}${normalizedTitle}`;
    }
  }

  const rottenTomatoes = mediaData.Ratings && mediaData.Ratings.find(rating => rating.Source === "Rotten Tomatoes");
  let rottenTomatoesRating = "";
  if (rottenTomatoes) {
    const rtValue = parseInt(rottenTomatoes.Value.replace('%', ''));
    const iconId = rtValue >= 60 ? "rt" : "rtsplat";
    const linkClass = rtValue >= 60 ? "rt" : "rtsplat";

    rottenTomatoesRating = `
    <a href="${rtUrl}" target="_blank" class="rating-link ${linkClass}">
      <span class="rating-icon rt">
        <svg class="rt-icon">
          <use href="#${iconId}"/>
        </svg>
        <span class="rating">${rottenTomatoes.Value}</span>
      </span>
      <span class="ttip-txt" data-tt-pos="bottom">🔗 Rotten Tomatoes</span>
    </a>`;
  }
  
  // Combine all ratings into a single HTML snippet
  return `
    <span class="ratings">
      <span class="rating-pill" data-tooltip>${imdbRatingLink}</span>
      <span class="rating-pill" data-tooltip>${tmdbRatingLink}</span>
    ${metascoreIcon ? `<span class="rating-pill" data-tooltip>${metascoreIcon}</span>` : ""}
    ${rottenTomatoesRating ? `<span class="rating-pill" data-tooltip>${rottenTomatoesRating}</span>` : ""}
  </span>
  `;
}

function generateCustomListsHtml(mediaData, customListsMeta) {
  const mediaLists = mediaData.lists || {};
  if (!customListsMeta || customListsMeta.length === 0) {
    return '<ul class="ttip-lists"><li class="ttip-list-empty">No custom lists</li></ul>';
  }
  let html = '<ul class="ttip-lists">';
  for (const listMeta of customListsMeta) {
    const isInList = !!mediaLists[listMeta.list];
    const activeClass = isInList ? ' active' : '';
    html += `<li class="ttip-list-entry${activeClass}" data-list="${listMeta.list}" data-label="${escapeHTML(listMeta.label)}" data-imdbid="${mediaData.imdbID}" tabindex="0">
      <span class="ttip-list-label">${escapeHTML(listMeta.label)}</span>
    </li>`;
  }
  html += '</ul>';
  return html;
}

function refreshCustomListsInTooltips(imdbID = null) {
  // If imdbID is provided, only update that specific card
  const selector = imdbID ? 
    `.media-card[data-imdbid="${imdbID}"]` : 
    '.media-card';
    
  document.querySelectorAll(selector).forEach(mediaCard => {
    const cardImdbID = mediaCard.dataset.imdbid;
    if (!cardImdbID) return;

    const mediaData = mediaCache.get(cardImdbID);
    if (!mediaData) return;

    // Generate new custom lists HTML
    const customListsHtml = generateCustomListsHtml(mediaData, customListsMeta);

    // Find all tooltips for this card
    const tooltips = mediaCard.querySelectorAll('.tooltip-text');
    tooltips.forEach(tooltip => {
      // Find the existing custom lists container
      const oldCustomLists = tooltip.querySelector('.ttip-lists');
      if (oldCustomLists) {
        // Replace the old custom lists HTML
        oldCustomLists.outerHTML = customListsHtml;
      }
    });
  });
}

/**
 * UI: Generate tooltip for movie cards in grid
 * @param {*} mediaData 
 * @returns 
 */
function generateTooltip(mediaData) {

  // const imdbID = mediaData.imdbID;

  // Format arrays for display (comma-separated)
  const formatArrayForDisplay = (array, label) => {
    // Check if the input is an array
    if (!Array.isArray(array)) {
      return `<span style="color: red;"><strong>Warning:</strong> ${label} is not an array!</span>`;
    }

    // Handle empty arrays gracefully
    return array.length > 0
      ? `<span><strong>${label}:</strong> ${array.join(", ")}</span>`
      : `<span><strong>${label}:</strong> N/A</span>`;
  };

  // Convert an array to a sorted, comma-separated string
  const arrayToCommaSeparatedString = (array) => {
    if (Array.isArray(array) && array.length > 0) {
      return array.sort((a, b) => a.localeCompare(b)).join(", ");
    }
    return "N/A";
  };

  // Generate movie details inline with the specified format
  // const originalTitle = mediaData.original_title ? `<span class="original-title"><em>${mediaData.original_title}</em></span>` : "";

  // Use only the first country if more than one is listed
  const country = Array.isArray(mediaData.Country) && mediaData.Country.length > 0
    ? (["United States of America", "USA"].includes(mediaData.Country[0])
      ? "United States"
      : mediaData.Country[0])
    : "N/A";

  // const languages = arrayToCommaSeparatedString(mediaData.Language);
  const genres = arrayToCommaSeparatedString(mediaData.Genre);

  // Use movieData.Rated if not "N/A", otherwise use a fallback certification
  const certification =
    mediaData.Rated !== "N/A"
      ? mediaData.Rated
      : mediaData.certification || "N/A";

  const ratings = generateRatingsHtml(mediaData);
  const plot = `<span>${mediaData.Plot}</span>`;
  const director = `${formatArrayForDisplay(mediaData.Director, "Director")}`;
  const writers = `${formatArrayForDisplay(mediaData.Writer, "Writers")}`;
  const actors = `${formatArrayForDisplay(mediaData.Actors, "Actors")}`;
  const awards = mediaData.Awards && mediaData.Awards !== "N/A" ? `<span><strong>Awards:</strong> ${mediaData.Awards.replace(/(award)(\d)| total$/gi, (_, g1, g2) => g1 ? `${g1}. ${g2}` : '')}</span>` : "";
  const boxOffice = mediaData.BoxOffice && mediaData.BoxOffice !== "N/A" ? `<span><strong>Box Office:</strong> ${mediaData.BoxOffice}</span>` : "";

  // Construct the TMDB JustWatch URL using the tmdbID and the global country.
  const tmdbJustWatchUrl = `${tmdbBaseUrl}${mediaData.mediaType}/${mediaData.tmdbID}/watch?locale=${userSettings.justwatch_country}`;

  const customListsHtml = generateCustomListsHtml(mediaData, customListsMeta);
  
  // Build the tooltip list content.
  let tooltipListContent = '';

  // Common function to generate trailer links
  const generateTrailerLinks = (trailers) => trailers
    .map(trailer => `
    <li class="url-line">
      <div class="full-link"
           data-video-key="${trailer.key}"
           data-media-type="trailer">
        <span class="url-ttip-txt">${trailer.name || 'Trailer'}</span>
      </div>
    </li>`
    ).join('');

  // Always add JustWatch link first
  tooltipListContent = `
<li class="url-line justwatch-link">
  <a href="${tmdbJustWatchUrl}" target="_blank" class="justwatch-link">
    <span class="url-ttip-txt"><svg class="justwatch-icon"><use href="#justwatch_icon" /></svg><span class="justwatch-icon">JustWatch</span></span>
  </a>
</li>`;


  // Add trailers if they exist (regardless of justwatch_links setting)
  if (mediaData.trailers?.length) {
    tooltipListContent += generateTrailerLinks(mediaData.trailers);
  }

  // Build the final HTML markup.
  const urls = `
  <div class="play-button" data-tooltip>
    <div class="play-container">
      <a href="${tmdbJustWatchUrl}" target="_blank" class="play-link">
        <svg class="play-icon"><use href="#play_icon"/></svg>
      </a>
      <div class="urls-ttip-content">
        <ul class="urls-list">
          ${tooltipListContent}
        </ul>
      </div>
    </div>
  </div>
`;

  // Gallery Button
  const galleryButton = `
          <button id="gallery-media" class="ttip-button gallery-media" disabled>
<svg class="ttip-btn-icon"><use href="#gallery"/></svg>
</button>
      `;

  // Gallery Button
  const shareButton = `
          <button id="share-media" class="ttip-button share-media" disabled>
      <svg class="ttip-btn-icon"><use href="#share"/></svg>
     </button>
      `;

  // Note Button
  const setButton = `
          <button id="add-set" class="ttip-button add-set" disabled>
<svg class="ttip-btn-icon"><use href="#set"/></svg></button>
      `;

  // Refresh Button
  const refreshButton = `
          <button id="refresh-media" class="ttip-button refresh-media">
<svg class="ttip-btn-icon"><use href="#refresh"/></svg>
          </button>
      `;

  const noteButton = `
            <button id="note-media" class="ttip-button note-media">
          <svg class="ttip-btn-icon"><use href="#edit"/></svg>
            </button>
      `;

  const listButton = `
            <button id="list-media" class="ttip-button list-media" data-state="inactive" data-tooltip>
          <svg class="ttip-btn-icon"><use href="#list_custom"/></svg>
            </button>
      `;

  const deleteButton = `
            <button id="delete-media" class="ttip-button delete-media"><svg class="ttip-btn-icon"><use href="#trash"/></svg>
            </button>
      `;

  return `
          <div class="detail-panel-container">
            <div class="detail-panel">
                <svg class="detail-icon"><use href="#detail_view"/></svg>
            </div>
          </div>
            <div class="top-row">
                <div class="title-container">
                  <span class="title">${mediaData.Title}</span>
                </div>
                  <div class="release-date-container">
                    <span class="rated">${certification}</span> 
                    ${mediaData.Runtime && mediaData.Runtime !== "N/A" ? `${mediaData.Runtime} &bull; ` : ''}
                    ${mediaData.Year} ${country}
                  </div>
                  <div class="details-container">${genres} 
                </div>
                <div class="ratings-container">
                  ${ratings}
                </div>
              </div>
            <div class="middle-row">
              ${plot}
              ${director}
              ${writers}
              ${actors}
              ${awards}
              ${boxOffice}
            </div>
             <div class="bottom-row">
             <div class="left-section">
              
                  <!--div class="gallery-media" data-tooltip>${galleryButton}
                    <span class="ttip-txt" data-tt-pos="top">Gallery (NYI)</span>
                  </div>
                  <div class="gallery-media" data-tooltip>${setButton}
                    <span class="ttip-txt" data-tt-pos="top">Add to Set (NYI)</span>
                  </div--> 
                  <div class="note-media" data-tooltip>${noteButton}
                    <span class="ttip-txt" data-tt-pos="top">Add Note</span>
                  </div>
                  <div class="list-media" data-tooltip>${listButton}
                    <span class="ttip-txt lists" data-tt-pos="top">${customListsHtml}</span>
                  </div>
          
             </div>
              <div class="middle-section">${urls}</div>
              <div class="right-section">
                <div class="delete-media" data-tooltip>${deleteButton}
                  <span class="ttip-txt" data-tt-pos="top">Delete</span>
                </div>  
                  <!--div class="gallery-media" data-tooltip>${shareButton}
                    <span class="ttip-txt" data-tt-pos="top">Share (NYI)</span>
                  </div-->
                <div class="refresh-media" data-tooltip>${refreshButton}
                <span class="ttip-txt" data-tt-pos="top">Refresh</span>
                </div>
            </div>
        `;
}

/**
 * Update the state of a filter button.
 * @param {HTMLElement} button - The filter button element.
 */
function updateFilterButtonState(button) {
  if (!button) return;

  let isActive = false;

  // Check if ratings filter is active
  const isRatingFilterActive =
    (activeMinImdb !== null && activeMinImdb > 0) ||
    (activeMaxImdb !== null && activeMaxImdb < 10) ||
    (activeMinTmdb !== null && activeMinTmdb > 0) ||
    (activeMaxTmdb !== null && activeMaxTmdb < 10) ||
    (activeMinMetascore !== null && activeMinMetascore > 0) ||
    (activeMaxMetascore !== null && activeMaxMetascore < 100) ||
    (activeMinRT !== null && activeMinRT > 0) ||
    (activeMaxRT !== null && activeMaxRT < 100);

  // Check if genre filter is active
  const isGenreFilterActive = Array.isArray(activeGenres) && activeGenres.length > 0;

  // Check if year filter is active
  const isYearFilterActive =
    (activeStartYear !== null && activeStartYear > defaultStartYear) ||
    (activeEndYear !== null && activeEndYear < defaultEndYear);

  // Check if list filters are active // ### check if we need it...
  // const isListFilterActive = Array.isArray(activeLists) && activeLists.length > 0;

  // Check if country filter is active
  const isCountryFilterActive = activeCountry && activeCountry !== "All";

  // Determine button state based on its ID
  switch (button.id) {
    case "ratings-filter-button":
      isActive = isRatingFilterActive;
      break;
    case "genre-filter-button":
      isActive = isGenreFilterActive;
      break;
    case "year-filter-button":
      isActive = isYearFilterActive;
      break;
    case "dropdown-button-country":
      isActive = isCountryFilterActive;
      break;
    case "favourites-filter-icon":
    case "watchlist-filter-icon":
    case "collection-filter-icon":
      isActive = activeLists.some(item => item.name === button.id.replace("-filter-icon", "") && item.type === 'core');
      break;
    case "customlist-filter-icon": {
      const panel = document.getElementById('customlist-dropdown-panel');
      // Keep icon active if panel is open, OR if any custom list filter is active
      isActive = (panel && !panel.classList.contains('hidden')) ||
        activeLists.some(item => item.type === 'custom');
      break;
    }
  }

  // Toggle the "active" class based on the state
  if (isActive) {
    button.classList.add("active");
  } else {
    button.classList.remove("active");
  }
}

/**
 * Debounce function to limit the frequency of function calls.
 * @param {Function} func - The function to debounce.
 * @param {number} delay - The debounce delay in milliseconds.
 * @param {boolean} [useGlobal=false] - Whether to use a global timeout (for shared state).
 * @returns {Function} - A debounced version of the original function.
 */
function debounce(func, delay, useGlobal = false) {
  let timeout; // Scoped timeout for individual instances
  if (useGlobal) {
    window.debounceTimeout = window.debounceTimeout || {}; // Global timeout storage
  }

  return function (...args) {
    const context = this;

    // Clear existing timeout
    if (useGlobal) {
      clearTimeout(window.debounceTimeout[func]);
      window.debounceTimeout[func] = setTimeout(() => func.apply(context, args), delay);
    } else {
      clearTimeout(timeout);
      timeout = setTimeout(() => func.apply(context, args), delay);
    }
  };
}

/**
 * Throttles a function to limit how often it can be called
 * @param {Function} func - The function to throttle
 * @param {number} limit - The time limit in milliseconds
 * @returns {Function} - The throttled function
 */
function throttle(func, limit) {
  let inThrottle = false;
  let lastFunc;

  return function (...args) {
    const context = this;

    if (!inThrottle) {
      // If not currently throttled, execute immediately
      func.apply(context, args);
      inThrottle = true;

      // Reset throttle flag after the limit
      setTimeout(() => {
        inThrottle = false;

        // Execute the last call if one was made during throttle
        if (lastFunc) {
          lastFunc();
          lastFunc = null;
        }
      }, limit);
    } else {
      // Store the latest call to execute after throttle period
      lastFunc = function () {
        func.apply(context, args);
      };
    }
  };
}

/**
 * Builds a letter index for efficient jumping
 * @param {Array} records - The filtered records to index
 * @returns {Map} A map of first letters to record indices
 */
function buildLetterIndex() {
  if (!filteredRecords || filteredRecords.length === 0) return null;

  //console.log("Building letter index for quick navigation...");
  const index = new Map();

  // Build the index mapping first letters to their positions
  filteredRecords.forEach((record, i) => {
    if (!record || !record.Title) return;

    // Get first character and normalize it to lowercase
    const firstChar = normalizeForSort(record.Title).charAt(0).toLowerCase();

    // Group numeric titles under "0"
    const indexChar = /^\d/.test(firstChar) ? "0" : firstChar;

    // Store only the first occurrence of each letter
    if (!index.has(indexChar)) {
      index.set(indexChar, i);
      //console.log(`Letter index: "${indexChar}" starts at position ${i} (${record.Title})`);
    }
  });

  // console.log(`Built letter index with ${index.size} unique starting characters`);
  return index;
}

/**
 * Internal function that performs the actual page size calculation
 * @private
 * @param {number} cardWidth - The width of each card
 * @returns {number} The calculated page size
 */
function calculatePageSizeInternal(cardWidth) {
  // Calculate viewport dimensions
  const viewportWidth = window.innerWidth - 60; // Account for margins/padding
  const viewportHeight = window.innerHeight - 120; // Account for header/footer

  // Calculate grid metrics
  const gap = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--grid-gap')) || 10;
  const cardsPerRow = Math.max(1, Math.floor((viewportWidth + gap) / (cardWidth + gap)));
  const rowHeight = (cardWidth * 1.5) + gap; // Card height is typically 1.5x width for posters
  const visibleRows = Math.max(1, Math.ceil(viewportHeight / rowHeight));

  // Base calculation: visible area plus buffer
  let calculatedPageSize = cardsPerRow * visibleRows;

  // Apply size-based multipliers with minimums directly based on card width
  if (cardWidth <= 80) {
    // Very small cards (50-80px) - show more cards
    calculatedPageSize = Math.max(200, calculatedPageSize * 3);
  } else if (cardWidth <= 150) {
    // Small cards (81-150px) - show more cards but fewer than tiny
    calculatedPageSize = Math.max(75, calculatedPageSize * 2);
  } else if (cardWidth <= 200) {
    // Medium cards (151-200px) - show slightly more cards
    calculatedPageSize = Math.max(36, calculatedPageSize * 1.75);
  } else {
    // Large cards (201-300px) - standard multiplier
    calculatedPageSize = Math.max(24, calculatedPageSize * 1.5);
  }

  // Round to multiple of cardsPerRow for clean rows
  calculatedPageSize = Math.ceil(calculatedPageSize / cardsPerRow) * cardsPerRow;

  console.log(`Calculated page size: ${calculatedPageSize} (${cardsPerRow} cards/row × ${visibleRows} rows, card width: ${cardWidth}px)`);
  return calculatedPageSize;
}

/**
 * Public function to get the cached page size
 * @returns {number} The calculated page size
 */
function getPageSize() {
  const gridSizeSlider = document.getElementById('gridSizeSlider');
  const cardWidth = gridSizeSlider ? parseInt(gridSizeSlider.value, 10) : DEFAULT_CARD_SIZE;

  // Use the cache system
  return pageSizeCache.get(cardWidth);
}

/**
 * Resets the grid and optionally resets pagination
 * @param {boolean} resetPagination - Whether to reset pagination counters
 */
function resetGrid(resetPagination = true) {
  const cardGrid = document.querySelector(".card-grid");
  if (!cardGrid) {
    console.error("Card grid not found.");
    return;
  }

  // Clear DOM and disconnect observers
  cardGrid.innerHTML = "";
  if (observer) {
    observer.disconnect();
    observer = null;
  }

  isObserving = false;

  if (resetPagination) {
    renderedMediaIDs.clear(); // Prevent duplicate renders
    //currentPage = 0;
    startIndex = 0;
    endIndex = 0;
    // console.log("Full pagination reset");
  }
}

/**
 * Jump to a specific letter in the grid
 * @param {string} letter - The letter to jump to
 */
function jumpToLetter(letter) {
  const normalizedLetter = letter.toLowerCase();
  //console.log(`Jump request to letter: ${normalizedLetter}`);

  if (!filteredRecords || filteredRecords.length === 0) return;

  // Skip expensive operations if we already have the index
  // use letterIndex = null; // to force rebuild on next jump, this is maybe required in some functions  ###
  if (!letterIndex) {
    filteredRecords = sortRecords(filteredRecords, "title", true);
    letterIndex = buildLetterIndex();
  }

  // Find target index
  let targetIndex;
  if (normalizedLetter === "0" && letterIndex?.has("0")) {
    targetIndex = letterIndex.get("0");
  } else if (letterIndex?.has(normalizedLetter)) {
    targetIndex = letterIndex.get(normalizedLetter);
  } else {
    targetIndex = filteredRecords.findIndex(media =>
      normalizeForSort(media.Title).charAt(0).toLowerCase() === normalizedLetter
    );
  }

  if (targetIndex === -1) {
    showNotification(`No titles starting with "${letter}".`, false);
    return;
  }

  // Skip if already at this letter and index
  if (currentJumpLetter === normalizedLetter && currentJumpIndex === targetIndex) return;

  // Set jump flags
  isJumping = true;
  jumpLock = true;

  // Disconnect observers during jump
  if (observer) {
    observer.disconnect();
    observer = null;
  }

  // Store menuBarHeight for consistent use across promises
  const menuBarHeight = getMenuBarHeight();

  // Use Promise chain to separate DOM reads from writes
  Promise.resolve().then(() => {
    // First microtask: Read layout properties
    const viewportTop = window.scrollY + menuBarHeight;
    const viewportBottom = viewportTop + window.innerHeight - menuBarHeight;

    // Get target position
    let targetPos;
    if (filteredRecords[targetIndex].element) {
      const weakRef = filteredRecords[targetIndex].element;
      const element = weakRef instanceof WeakRef ? weakRef.deref() : weakRef;
      targetPos = element?.offsetTop;
    }
    
    if (!targetPos) {
      const cardHeight = document.querySelector(".media-card")?.offsetHeight || 300;
      targetPos = targetIndex * cardHeight;
    }

    // Determine if reset is needed
    const needsReset = targetPos < viewportTop || targetPos > viewportBottom;
    
    return { needsReset, targetIndex };
  }).then(({ needsReset, targetIndex }) => {
    // Second microtask: Perform DOM modifications
    if (needsReset) {
      resetGrid(false);
      renderedMediaIDs.clear();

      startIndex = Math.max(0, targetIndex - Math.floor(pageSize / 2));
      endIndex = Math.min(filteredRecords.length, startIndex + pageSize);
      //currentPage = Math.floor(startIndex / pageSize);
      
      renderCard(filteredRecords.slice(startIndex, endIndex));
    }
    
    // Update tracking variables
    currentJumpLetter = normalizedLetter;
    currentJumpIndex = targetIndex;
    
    return { targetIndex };
  }).then(({ targetIndex }) => {
    // Third microtask: Find target card and prepare for scrolling
    return new Promise(resolve => {
      // Use setTimeout to match original timing
      setTimeout(() => {
        const cardGrid = document.querySelector(".card-grid");
        const mediaCards = cardGrid.querySelectorAll(".media-card");

        if (!mediaCards || mediaCards.length === 0) {
          isJumping = false;
          jumpLock = false;
          return;
        }

        // Calculate the local index of the target
        const localIndex = targetIndex - startIndex;
        const boundedLocalIndex = Math.min(Math.max(0, localIndex), mediaCards.length - 1);
        const targetCard = mediaCards[boundedLocalIndex];

        if (!targetCard) {
          isJumping = false;
          jumpLock = false;
          return;
        }

        // Calculate position with the same 10px buffer as original
        const targetY = targetCard.getBoundingClientRect().top + window.scrollY - menuBarHeight;

        resolve({ targetCard, targetY });
      }, 150); // Same timeout as original
    });
  }).then(({ targetCard, targetY }) => {
    // Fourth microtask: Handle scrolling and cleanup
    if (!targetCard) return;

    // Scroll to position and highlight the target card
    window.scrollTo({
      top: targetY,
      behavior: 'smooth'
    });

    targetCard.classList.add('jump-target');
    
    // Use the same timeout durations as the original
    setTimeout(() => {
      targetCard.classList.remove('jump-target');
    }, 2000);

    // Release jump locks after the same delay
    setTimeout(() => {
      isJumping = false;
      jumpLock = false;
      observeMediaCards();
    }, 1000);
  });
}

/**
 * Set up observers for lazy loading with scroll detection
 */
function observeMediaCards() {
  // Always disconnect existing observer first
  if (observer) {
    observer.disconnect();
    observer = null;
  }

  const mediaGrid = document.querySelector(".card-grid");
  if (!mediaGrid) {
    console.error("Media grid not found for observation");
    return;
  }

  // Get fresh references to first and last cards
  const firstCard = mediaGrid.querySelector(".media-card:first-child");
  const lastCard = mediaGrid.querySelector(".media-card:last-child");

  if (!firstCard || !lastCard) {
    console.warn("Cannot observe cards: first or last card not found");
    return;
  }

  // console.log(`Setting up observers: first card index ${startIndex}, last card index ${endIndex - 1}`);

  // Create a new observer with improved rootMargin for better detection
  observer = new IntersectionObserver(
    entries => {
      if (isJumping || jumpLock) return;
      
      // Apply throttling to batch loading
      const now = Date.now();
      if (now - lastBatchLoadTime < BATCH_THROTTLE_INTERVAL) return;

      entries.forEach(entry => {
        if (entry.isIntersecting) {
          if (entry.target === lastCard && endIndex < filteredRecords.length) {
            // console.log("Last card visible, loading next batch");
            lastBatchLoadTime = now;
            loadNextBatch();
          } else if (entry.target === firstCard && startIndex > 0) {
            // console.log("First card visible, loading previous batch");
            lastBatchLoadTime = now;
            loadPreviousBatch();
          }
        }
      });
    },
    {
      rootMargin: "300px 0px", // Increased vertical margin for earlier detection
      threshold: 0.1
    }
  );

  // Explicitly observe both cards
  observer.observe(firstCard);
  observer.observe(lastCard);

  // console.log(`Observers attached to cards at indices ${startIndex} and ${endIndex - 1}`);
}

/**
 * Load previous batch of records with consistent index management
 */
function loadPreviousBatch() {
  if (isJumping || jumpLock) {
    console.log("Skipping lazy loading of previous batch due to jump lock.");
    return;
  }

  if (startIndex <= 0) {
    console.log("No previous pages to load.");
    return;
  }

  // Temporarily disconnect observer to prevent concurrent operations
  if (observer) {
    observer.disconnect();
    observer = null;
  }

  // Read all DOM measurements BEFORE any modifications
  const scrollTop = window.scrollY;
  const oldFirstCard = document.querySelector(".media-card:first-child");
  const oldFirstCardTop = oldFirstCard ? oldFirstCard.getBoundingClientRect().top : 0;

  const previousStart = Math.max(0, startIndex - pageSize);
  const previousEnd = startIndex;
  //console.log(`Lazy loading previous batch: indices ${previousStart} to ${previousEnd}`);

  const paginatedRecords = filteredRecords.slice(previousStart, previousEnd);
  if (paginatedRecords.length > 0) {
    // Update index BEFORE rendering
    startIndex = previousStart;

    // Render the cards
    renderCard(paginatedRecords, "prepend");

    // Use Promise.resolve() to give the browser a chance to process layout changes
    Promise.resolve().then(() => {
      if (oldFirstCard) {
        const newFirstCardTop = oldFirstCard.getBoundingClientRect().top;
        window.scrollTo(0, scrollTop + (newFirstCardTop - oldFirstCardTop));
      }
      
      // Ensure we set up observers after scroll adjustment
      observeMediaCards();
    });
  }
}

/**
 * Load next batch of records with consistent index management
 */
function loadNextBatch() {
  if (isJumping || jumpLock) {
    console.log("Skipping lazy loading of next batch due to jump lock.");
    return;
  }

  if (endIndex >= filteredRecords.length) {
    console.log("No more titles to load.");
    return;
  }

  // Calculate next batch indices
  const nextStart = endIndex;
  const nextEnd = Math.min(filteredRecords.length, endIndex + pageSize);
  //console.log(`Lazy loading next batch: indices ${nextStart} to ${nextEnd}`);

  const paginatedRecords = filteredRecords.slice(nextStart, nextEnd);
  if (paginatedRecords.length > 0) {
    // Update index BEFORE rendering
    endIndex = nextEnd;

    // Render the cards
    renderCard(paginatedRecords, "append");

    // console.log(`After next batch: startIndex=${startIndex}, endIndex=${endIndex}`);
    observeMediaCards();
  }
}

/**
 * Load a specific page of records
 * @param {number} startIdx - The starting index to load from
 */
function loadPaginatedRecords(startIdx = 0) {
  // Batch all calculations first
  const endIdx = Math.min(filteredRecords.length, startIdx + pageSize);
  const batch = filteredRecords.slice(startIdx, endIdx);
  
  if (batch.length === 0) return;
  
  // Update indices before DOM operations
  startIndex = startIdx;
  endIndex = endIdx;
  //currentPage = Math.floor(endIdx / pageSize);
  
  // Then perform DOM operations
  if (startIdx === 0) {
    resetGrid(false);
    renderedMediaIDs.clear();
  }
  
  // Render cards and observe in a single animation frame
  requestAnimationFrame(() => {
    renderCard(batch);
    observeMediaCards();
  });
}

/**
 * Renders media cards in the grid
 * @param {Array} records - The records to render
 * @param {string} mode - The render mode: 'append', 'prepend', or 'reset'
 */
function renderCard(records, mode = 'append') {
  const cardGrid = document.querySelector('.card-grid');
  if (!cardGrid) {
    console.error("Card grid not found.");
    return;
  }

  // Clear grid if mode is reset
  if (mode === 'reset') {
    cardGrid.innerHTML = '';
  }

  // Create fragment for batch DOM operations
  const fragment = document.createDocumentFragment();
  
  // Read DOM measurements once before modifications
  const gridSizeSlider = document.getElementById('gridSizeSlider');
  const cardWidth = gridSizeSlider ? parseInt(gridSizeSlider.value, 10) : DEFAULT_CARD_SIZE;
  const isAtMaxSize = cardWidth >= (gridSizeSlider ? parseInt(gridSizeSlider.max, 10) : 300);
  
  // Save scroll position before DOM changes (for prepend mode)
  const scrollTop = window.scrollY;
  const oldFirstCard = mode === 'prepend' ? cardGrid.querySelector('.media-card:first-child') : null;
  const oldFirstCardTop = oldFirstCard ? oldFirstCard.getBoundingClientRect().top : 0;

  // Create placeholder SVG once for reuse
  const posterHeight = Math.round(cardWidth * 1.5);
  const placeholderSVG = createPlaceholderSVG(cardWidth, posterHeight, 'poster');

  // Process all records in a single batch
  records.forEach((media, index) => {
    // Skip already rendered cards unless we're in reset mode
    if (mode !== 'reset' && renderedMediaIDs.has(media.imdbID)) return;
    renderedMediaIDs.add(media.imdbID);

    const mediaCard = document.createElement('div');
    mediaCard.className = 'media-card';
    mediaCard.dataset.imdbid = media.imdbID;
    mediaCard.dataset.tmdbid = media.tmdbID || '';
    mediaCard.dataset.mediaType = media.mediaType || 'movie';
    mediaCard.dataset.mediaTitle = media.Title || '';
    mediaCard.dataset.index = (startIndex + index).toString();

    // Store element reference as we create it
    if (typeof WeakRef !== 'undefined') {
      media.element = new WeakRef(mediaCard);
    } else {
      media.element = mediaCard;
    }

    if (media.isJumpTarget) {
      mediaCard.dataset.jumpTarget = 'true';
      mediaCard.classList.add('jump-target');
    }

    const mediaContainer = document.createElement('div');
    const mediaTitle = media.Title || media.name || "Unknown Title";
    const mediaYear = media.Year || (media.firstairdate ? media.firstairdate.substring(0, 4) : '');

    if (media.Poster && media.Poster !== 'N/A') {
      const isCustomPoster = media.defaultPoster && media.Poster !== media.defaultPoster;
      const imageType = isCustomPoster ? 'custom_poster' : 'poster';
      
      const img = document.createElement('img');
      img.alt = mediaTitle + (mediaYear ? ' ' + mediaYear : '');
      img.loading = 'lazy';
      img.dataset.isCustom = isCustomPoster ? 'true' : 'false';
      img.dataset.imageType = imageType;
      img.dataset.cardWidth = cardWidth.toString();

      // Determine URLs once
      const posterUrl = media.Poster.startsWith('https://m.media-amazon.com') ?
        media.Poster : buildTMDbImageUrl(media.Poster, 'poster');
      const defaultPosterUrl = media.defaultPoster ?
        buildTMDbImageUrl(media.defaultPoster, 'poster') : null;
      
      img.dataset.originalUrl = posterUrl;
      img.dataset.defaultUrl = defaultPosterUrl || '';

      // Set image source
      if (isCustomPoster) {
        img.src = posterUrl;
      } else if (posterUrl.startsWith('https://m.media-amazon.com')) {
        // Amazon images - use as is
        img.src = posterUrl;
      } else if (posterUrl.includes('tmdb.org/t/p/')) {
        // TMDB URL that already has a size - replace with w342
        img.src = posterUrl.replace(/\/t\/p\/\w+\//, '/t/p/w342/');
      } else {
        // TMDB image path without size - add w342
        img.src = `https://image.tmdb.org/t/p/w342${posterUrl.startsWith('/') ? '' : '/'}${posterUrl}`;
      }

      // Handle image errors
      img.onerror = () => {
        if (defaultPosterUrl) {
          if (defaultPosterUrl.startsWith('https://m.media-amazon.com')) {
            // Amazon images - use as is
            img.src = defaultPosterUrl;
          } else if (defaultPosterUrl.includes('tmdb.org/t/p/')) {
            // TMDB URL that already has a size - replace with w342
            img.src = defaultPosterUrl.replace(/\/t\/p\/\w+\//, '/t/p/w342/');
          } else {
            // TMDB image path without size - add w342
            img.src = `https://image.tmdb.org/t/p/w342${defaultPosterUrl.startsWith('/') ? '' : '/'}${defaultPosterUrl}`;
          }
          
          img.onerror = () => {
            mediaContainer.className = 'placeholder-poster';
            mediaContainer.innerHTML = placeholderSVG;
          };
        } else {
          mediaContainer.className = 'placeholder-poster';
          mediaContainer.innerHTML = placeholderSVG;
        }
      };
      
      mediaContainer.className = 'media-container';
      mediaContainer.appendChild(img);
    } else {
      mediaContainer.className = 'placeholder-poster';
      mediaContainer.innerHTML = placeholderSVG;
    }

    mediaCard.appendChild(mediaContainer);

    // Only add tooltips and icons at max size
    if (isAtMaxSize) {
      // Add tooltip
      const tooltip = document.createElement('div');
      tooltip.className = 'tooltip-text';
      tooltip.innerHTML = generateTooltip(media);
      mediaCard.appendChild(tooltip);
      
      // Add icons
      const topRowIcons = createTopRowIcons(media.imdbID);
      if (topRowIcons) {
        mediaCard.appendChild(topRowIcons);
      }
      
      initTooltips(mediaCard, media);

      // Mark as processed
      mediaCard.classList.add('processed');
    } else {
      // For smaller sizes, just add a flag for lazy loading
      mediaCard.dataset.needsTooltip = 'true';
    }
    
    // Mark as observed
    mediaCard.classList.add('observed');
    
    fragment.appendChild(mediaCard);
  });

  // Apply all DOM changes at once
  if (mode === 'prepend') {
    // Temporarily disable scroll events to prevent interference
    const oldScrollHandler = window.onscroll;
    window.onscroll = null;
    
    // Add the new cards
    cardGrid.prepend(fragment);
    
    // Use requestAnimationFrame to adjust scroll position after render
    requestAnimationFrame(() => {
      if (oldFirstCard) {
        const newFirstCardTop = oldFirstCard.getBoundingClientRect().top;
        window.scrollTo({
          top: scrollTop + (newFirstCardTop - oldFirstCardTop),
          behavior: 'auto' // Use 'auto' to avoid additional animations
        });
      }
      
      // Restore scroll handler
      window.onscroll = oldScrollHandler;
    });
  } else {
    cardGrid.appendChild(fragment);
  }
}

/**
 * Sort Records
 */
function sortRecords(media, criteria = "title", ascending = true) {
  if (!Array.isArray(media) || media.length === 0) {
    console.log("No titles available to sort.");
    return [];
  }

  // Create a copy to avoid mutating the original array
  const sortedRecords = [...media];

  // For title sorting, we need to handle search results differently
  if (criteria === "title" && activeSearchQuery && activeSearchQuery.trim() !== "") {
    // If we're sorting search results, preserve the search priority order
    // (exact matches, starts with, contains) that was established in searchItems
    return sortedRecords;
  }

  return sortedRecords.sort((a, b) => {
    let valueA, valueB;

    switch (criteria) {
      case "title": {
        // Use raw titles for franchise detection
        const titleA = a.Title || "";
        const titleB = b.Title || "";
        
        // Check for franchise relationship (one title is a subset of the other plus a number)
        const franchiseMatch = isSameMovieFranchise(titleA, titleB);
        
        if (franchiseMatch) {
          // If they're part of the same franchise, sort by sequel number
          return ascending ? franchiseMatch : -franchiseMatch;
        }
        
        // If not part of same franchise, use normalized titles for standard sorting
        valueA = normalizeForSort(titleA);
        valueB = normalizeForSort(titleB);
        break;
      }
      case "year":
        valueA = a.numericYear || 0;
        valueB = b.numericYear || 0;
        break;
      case "imdb":
        valueA = parseFloat(a.Ratings?.find(r => r.Source === "Internet Movie Database")?.Value) || 0;
        valueB = parseFloat(b.Ratings?.find(r => r.Source === "Internet Movie Database")?.Value) || 0;
        break;
      case "tmdb":
        valueA = a.vote_average || 0;
        valueB = b.vote_average || 0;
        break;
      case "metascore":
        valueA = parseInt(a.Metascore) || 0;
        valueB = parseInt(b.Metascore) || 0;
        break;
      case "rt":
        valueA = parseInt(a.Ratings?.find(r => r.Source === "Rotten Tomatoes")?.Value) || 0;
        valueB = parseInt(b.Ratings?.find(r => r.Source === "Rotten Tomatoes")?.Value) || 0;
        break;
      case "weighted":
        valueA = weightedRating.calculate(a.imdbRating, a.imdbVotes);
        valueB = weightedRating.calculate(b.imdbRating, b.imdbVotes);
        break;
      default:
        console.error(`Unknown sorting criteria: ${criteria}`);
        return 0;
    }

    // Handle all sorting with the same approach
    if (criteria === "title") {
      return ascending ? 
        valueA.localeCompare(valueB, undefined, {numeric: true, sensitivity: 'base'}) : 
        valueB.localeCompare(valueA, undefined, {numeric: true, sensitivity: 'base'});
    }

    // Handle numeric comparisons for other criteria
    const comparison = valueA - valueB;
    return ascending ? comparison : -comparison;
  });
}

/**
 * Determines if two movie titles are part of the same franchise
 * and returns a sort value based on their sequel numbers
 * @param {string} titleA - First movie title
 * @param {string} titleB - Second movie title
 * @returns {number|null} - Positive if A comes before B, negative if B comes before A, null if not same franchise
 */
function isSameMovieFranchise(titleA, titleB) {
  // Clean titles for comparison (lowercase, no special chars)
  const cleanA = titleA.toLowerCase().replace(/[^\w\s]/g, "");
  const cleanB = titleB.toLowerCase().replace(/[^\w\s]/g, "");
  
  // Extract base name and sequel number
  const extractInfo = (title) => {
    // Match patterns like "Title 2", "Title II", "Title Part 2", etc.
    const match = title.match(/^(.+?)(?:\s+(?:part\s+)?(\d+|[ivx]+))?(?:\s*:.*)?$/i);
    if (match) {
      const baseName = match[1].trim();
      let sequelNum = match[2] ? match[2].toLowerCase() : null;
      
      // Convert Roman numerals to numbers if needed
      if (sequelNum && /^[ivx]+$/.test(sequelNum)) {
        const romanValues = { i: 1, v: 5, x: 10 };
        let value = 0;
        for (let i = 0; i < sequelNum.length; i++) {
          const current = romanValues[sequelNum[i]];
          const next = romanValues[sequelNum[i + 1]] || 0;
          value += current < next ? -current : current;
        }
        sequelNum = value.toString();
      }
      
      return { baseName, sequelNum: sequelNum ? parseInt(sequelNum, 10) : 0 };
    }
    return { baseName: title, sequelNum: 0 };
  };
  
  const infoA = extractInfo(cleanA);
  const infoB = extractInfo(cleanB);
  
  // Check if they share the same base name
  if (infoA.baseName === infoB.baseName) {
    // Sort by sequel number
    return infoA.sequelNum - infoB.sequelNum;
  }
  
  // Special case for "Anchorman" series which has a subtitle
  if (cleanA.startsWith("anchorman") && cleanB.startsWith("anchorman")) {
    // Extract numbers after "anchorman"
    const numA = cleanA.match(/anchorman\s+(\d+)/i);
    const numB = cleanB.match(/anchorman\s+(\d+)/i);
    
    const seqA = numA ? parseInt(numA[1], 10) : 0;
    const seqB = numB ? parseInt(numB[1], 10) : 0;
    
    return seqA - seqB;
  }
  
  // Not the same franchise
  return null;
}

/**
 * Enhanced function to convert roman numerals and spelled-out numbers to digits.
 * This applies to both standalone numerals and those with prefixes like "Part" or "Episode".
 */
function normalizeNumerals(title) {
  if (!title) return "";
  
  // Mapping for Roman numerals
  const romanMap = {
    ' i': ' 1',
    ' ii': ' 2',
    ' iii': ' 3',
    ' iv': ' 4',
    ' v': ' 5',
    ' vi': ' 6',
    ' vii': ' 7',
    ' viii': ' 8',
    ' ix': ' 9',
    ' x': ' 10'
  };

  // Mapping for spelled-out English numbers
  const englishMap = {
    'one': '1',
    'two': '2',
    'three': '3',
    'four': '4',
    'five': '5',
    'six': '6',
    'seven': '7',
    'eight': '8',
    'nine': '9',
    'ten': '10'
  };

  // Convert to lowercase for consistent processing
  let normalizedTitle = title.toLowerCase();
  
  // First, handle "Part X" or "Episode X" patterns
  normalizedTitle = normalizedTitle.replace(
    /\b(part|episode)\s+(i|ii|iii|iv|v|vi|vii|viii|ix|x|one|two|three|four|five|six|seven|eight|nine|ten)\b/g, 
    (match, prefix, numeral) => {
      // Check if it's a Roman numeral
      if (/^(i|ii|iii|iv|v|vi|vii|viii|ix|x)$/i.test(numeral)) {
        const num = romanMap[' ' + numeral];
        return prefix + num;
      } else {
        // It's a spelled-out number
        const num = englishMap[numeral];
        return prefix + ' ' + num;
      }
    }
  );
  
  // Then handle standalone Roman numerals at the end of titles or after a space
  // This is important for titles like "Halloween III" or "Star Wars V"
  normalizedTitle = normalizedTitle.replace(
    /\s(i|ii|iii|iv|v|vi|vii|viii|ix|x)\b/g,
    (match) => {
      return romanMap[match] || match;
    }
  );
  
  return normalizedTitle;
}

/* Normalization for names/keywords – no roman numerals or fraction replacements */
function normalizeForName(name) {
  if (typeof name !== "string") name = String(name);
  return name.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim();
}

/**
 * Normalize titles for sorting (removes leading articles)
 */
function normalizeForSort(title) {
  const withoutArticles = title.replace(/^(the |a |an )/i, "");
  return normalizeForSearch(withoutArticles);
}

function normalizeForSearch(title) {
  // Replace Roman numerals (for titles only)
  title = normalizeNumerals(title);

  const fractionReplacements = {
    '½': '1/2', '⅓': '1/3', '⅔': '2/3',
    '¼': '1/4', '¾': '3/4', '⅕': '1/5',
    '⅖': '2/5', '⅗': '3/5', '⅘': '4/5',
    '⅙': '1/6', '⅚': '5/6', '⅛': '1/8',
    '⅜': '3/8', '⅝': '5/8', '⅞': '7/8'
  };
  const superscriptMap = {
    '⁰': ' 0', '¹': ' 1', '²': ' 2', '³': ' 3',
    '⁴': ' 4', '⁵': ' 5', '⁶': ' 6', '⁷': ' 7',
    '⁸': ' 8', '⁹': ' 9'
  };
  
  return title
    .toLowerCase()
    .normalize("NFD") // Normalize Unicode characters (e.g., é → e + ́)
    .replace(/[\u0300-\u036f]/g, "") // Remove diacritical marks
    .replace(/[øæå]/g, (match) => ({ ø: "o", æ: "ae", å: "a" }[match]))
    .replace(/([a-zA-Z0-9])([½⅓⅔¼¾⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞])/g, "$1 $2") // Add space before fractions if preceded by a character
    .replace(/[½⅓⅔¼¾⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞]/g, (match) => fractionReplacements[match])
    .replace(/[⁰¹²³⁴⁵⁶⁷⁸⁹]/g, (match) => superscriptMap[match])
    .replace(/'(?=\d)/g, "") // Remove apostrophes in front of numbers
    .replace(/'(?=\s|$)/g, "") // Remove trailing apostrophes
    // .replace(/ - /g, " ") // Remove hyphens surrounded by spaces
    .replace(/[-–—]/g, " ")
    .replace(/(?<!\d):|:(?!\d)/g, "") // Remove colons not surrounded by digits
    .replace(/[^a-z0-9\s?$/:-]/g, "") // Preserve ?$/- and spaces; remove others like "!"
    .replace(/\s+/g, " ")
    .replace(/([a-zA-Z])(\d)/g, "$1 $2") // add space between a letter and a number (A2 = A 2)
    .trim();
}

/**
 * Generate navigation links dynamically.
 * @param {HTMLElement} navPanel - The navigation panel element.
 */
function generateNavLinks(navPanel) {

  const letters = ["0", ..."ABCDEFGHIJKLMNOPQRSTUVWXYZ"]; // Quicknav Bar

  letters.forEach((letter) => {
    const link = document.createElement("a");
    link.href = "#"; // Prevent default behavior
    link.className = "nav-link";
    link.dataset.filter = letter; // Attach filter for jumpToLetter()

    // Wrap letter in a span for hover effects
    const span = document.createElement("span");
    span.textContent = letter;

    link.appendChild(span);
    navPanel.appendChild(link);
  });
}

/**
 * Handle clicks on navigation links.
 * @param {Event} event - The click event.
 * @param {HTMLElement} element - The clicked element (when using event delegation).
 */
function handleNavClick(event, element) {
  if (element && element.classList.contains('nav-link')) {
    event.preventDefault();
    const filter = element.dataset.filter;
    //console.log(`Clicked on letter ${filter}`);
    throttledJumpToLetter(filter);
  }
}

/**
 * Handle mouse movement to slide in/out the navigation panel.
 * @param {HTMLElement} navPanel - The navigation panel element.
 * @param {Event} event - The mousemove event.
 */
function handleMouseMove(navPanel, event) {
  if (event.clientX <= 20) {
    navPanel.style.left = "0"; // Slide in panel
  } else if (!navPanel.matches(":hover")) {
    navPanel.style.left = "-60px"; // Slide out panel
  }
}

/**
 * Auto-scroll letters inside the navigation panel based on mouse movement.
 * @param {HTMLElement} navPanel - The navigation panel element.
 * @param {Event} event - The mousemove event.
 */
function handleMouseOverNav(navPanel, event) {
  const panelHeight = navPanel.offsetHeight;
  const mouseY = event.clientY - navPanel.getBoundingClientRect().top;

  // Calculate scroll position based on mouse position
  const scrollPercentage = mouseY / panelHeight; // Value between 0 and 1
  const maxScrollTop = navPanel.scrollHeight - panelHeight;

  navPanel.scrollTop = maxScrollTop * scrollPercentage;
}

/**
 * Add empty space above 'A' and below 'Z' in the navigation panel.
 * @param {HTMLElement} navPanel - The navigation panel element.
 */
function addEmptySpaceToNav(navPanel) {
  const topSpacer = document.createElement("div");
  topSpacer.style.height = "20px"; // Adjust as needed
  navPanel.prepend(topSpacer);

  const bottomSpacer = document.createElement("div");
  bottomSpacer.style.height = "20px"; // Adjust as needed
  navPanel.append(bottomSpacer);
}

/**
 * Initialize the navigation panel and attach event listeners.
 * @param {HTMLElement} navPanel - The navigation panel element.
 */
function initializeNavPanel(navPanelId = "navPanel") {
  const navPanel = document.getElementById(navPanelId);
  if (!navPanel) {
    throw new Error(`Navigation panel (${navPanelId}) not found in DOM.`);
  }

  generateNavLinks(navPanel);
  addEmptySpaceToNav(navPanel);

  document.addEventListener("mousemove", (event) => handleMouseMove(navPanel, event));
  navPanel.addEventListener("mousemove", (event) => handleMouseOverNav(navPanel, event));
}

function normalizeString(input) {
  if (!input || typeof input !== "string") return "";

  // Map of superscript numbers to their regular numeric equivalents
  const superscriptMap = {
    '⁰': ' 0',
    '¹': ' 1',
    '²': ' 2',
    '³': ' 3',
    '⁴': ' 4',
    '⁵': ' 5',
    '⁶': ' 6',
    '⁷': ' 7',
    '⁸': ' 8',
    '⁹': ' 9'
  };

  return input
    .toLowerCase()
    .normalize("NFD") // Normalize Unicode characters
    .replace(/[\u0300-\u036f]/g, "") // Remove diacritical marks
    .replace(/[()]/g, "") // Remove parentheses
    .replace(/ - /g, " ") // Remove hyphens surrounded by spaces
    .replace(/[⁰¹²³⁴⁵⁶⁷⁸⁹]/g, (match) => superscriptMap[match]) // Convert superscript numbers to regular numbers
    .replace(/'(?=\d)/g, "") // Remove apostrophes in front of numbers
    .replace(/'(?=\s|$)/g, "") // Remove trailing apostrophes
    .replace(/[^a-z0-9\s&'!?½⅓⅔¼¾⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞/øæå:$\u00A3\u20AC-]/g, "") // Remove unwanted characters except alphanumeric, spaces, &, apostrophes, exclamation marks, question marks, fractions, and hyphens
    .replace(/(?<!\d):|:(?!\d)/g, "") // Remove colons not surrounded by digits
    .replace(/\s+/g, " ") // Replace multiple spaces with a single space
    .trim(); // Trim leading/trailing spaces
}

/**
 * Get sorted genres from IndexedDB.
 * @param {IDBDatabase} db - The IndexedDB instance.
 * @returns {Promise<Array>} - A promise that resolves to an array of sorted genres.
 */
async function getSortedGenres(db) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("genres", "readonly");
    const store = transaction.objectStore("genres");

    // Exclude special records like "lastUpdated" if applicable
    const request = store.getAll();

    request.onsuccess = () => {
      const allRecords = request.result;

      // Sort alphabetically by name and exclude non-genre records if necessary
      const sortedGenres = allRecords.filter(record => record.genreID).sort((a, b) =>
        a.name.localeCompare(b.name)
      );

      resolve(sortedGenres);
    };

    request.onerror = (event) => {
      console.error("Error fetching sorted genres:", event.target.error);
      reject(event.target.error);
    };
  });
}

/**
 * DB: Load Genres from indexedDB into cache
 * @param {*} db 
 */
async function populateGenres(db) {
  const lastUpdatedKey = "genres_lastUpdated"; // Key to track last update in localStorage
  const currentDate = new Date();

  try {
    // Check if the genres store is empty
    const isEmpty = await isGenresStoreEmpty(db);

    if (isEmpty) {
      console.warn("Genres store is empty. Fetching genres...");
      await fetchAndUpdateGenres(db);
      localStorage.setItem(lastUpdatedKey, currentDate.toISOString());
    } else {
      // Check timestamp in localStorage
      const storedTimestamp = localStorage.getItem(lastUpdatedKey);
      const isOutdated =
        !storedTimestamp || (currentDate - new Date(storedTimestamp)) > 30 * 24 * 60 * 60 * 1000;

      if (isOutdated) {
        console.log("Genres data is stale. Fetching updated genres...");
        await fetchAndUpdateGenres(db);
        localStorage.setItem(lastUpdatedKey, currentDate.toISOString());
      } else {
        // console.log("Genres data is up-to-date. Loading from IndexedDB...");
        genreCache = await getSortedGenres(db); // Load genres from IndexedDB into cache
      }
    }

    // console.log("Genre cache initialized:", genreCache);
  } catch (error) {
    console.error("Error populating genres:", error.message);
  }
}

/**
 * Check if the genres store is empty.
 * @param {IDBDatabase} db - The IndexedDB instance.
 * @returns {Promise<boolean>} - True if the store is empty, false otherwise.
 */
async function isGenresStoreEmpty(db) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("genres", "readonly");
    const store = transaction.objectStore("genres");
    const countRequest = store.count();

    countRequest.onsuccess = () => {
      resolve(countRequest.result === 0); // Resolve true if count is 0
    };

    countRequest.onerror = (event) => {
      console.error("Error checking genres store:", event.target.error);
      reject(event.target.error);
    };
  });
}

/**
 * Fetch and update genres from TMDB API.
 * @param {IDBDatabase} db - The IndexedDB instance.
 */
async function fetchAndUpdateGenres(db) {

  async function fetchGenresFromAPI(endpoint) {
    const url = `${tmdbApiBaseUrl}${endpoint}?api_key=${apiKeyManager.getTmdbKey()}`;
    try {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Failed to fetch ${endpoint}: ${response.statusText}`);
      const data = await response.json();
      return data.genres; // Returns an array of genres
    } catch (error) {
      console.error(`Error fetching genres from ${endpoint}:`, error.message);
      throw error;
    }
  }

  try {
    // Fetch movie and TV genres
    const movieGenres = await fetchGenresFromAPI("genre/movie/list");
    const tvGenres = await fetchGenresFromAPI("genre/tv/list");

    // Map genres by ID, resolving conflicts by keeping only movie genres
    const genreMap = new Map();
    let conflicts = [];

    // Add movie genres to the map
    movieGenres.forEach((genre) => {
      genreMap.set(genre.id, { genreID: genre.id, name: genre.name });
    });

    // Add TV genres, resolving conflicts
    tvGenres.forEach((genre) => {
      if (genreMap.has(genre.id)) {
        const existingGenre = genreMap.get(genre.id);
        if (existingGenre.name !== genre.name) {
          // Conflict detected: Log it and keep the movie genre
          conflicts.push({
            id: genre.id,
            movieName: existingGenre.name,
            tvName: genre.name,
          });
        }
      } else {
        // No conflict: Add the TV genre as-is
        genreMap.set(genre.id, { genreID: genre.id, name: genre.name });
      }
    });

    // Log conflicts to the console
    if (conflicts.length > 0) {
      console.warn("Conflicts detected between movie and TV genres:", conflicts);
    } else {
      console.log("No conflicts detected between movie and TV genres.");
    }

    // Convert the map back to an array and sort alphabetically by name
    const allGenres = Array.from(genreMap.values()).sort((a, b) =>
      a.name.localeCompare(b.name)
    );

    console.log("Resolved and sorted genres:", allGenres);

    // Update global cache with sorted genres
    genreCache = allGenres;

    // Store sorted genres in IndexedDB
    const transaction = db.transaction("genres", "readwrite");
    const store = transaction.objectStore("genres");

    // Clear existing genres before updating
    store.clear();

    allGenres.forEach((genre) => {
      store.put(genre);
    });

    return new Promise((resolve, reject) => {
      transaction.oncomplete = () => {
        console.log("Populated 'genres' store with resolved and sorted movie and TV genres.");
        resolve();
      };

      transaction.onerror = (event) => {
        console.error("Failed to update 'genres' store:", event.target.error.message);
        reject(event.target.error);
      };
    });
  } catch (error) {
    console.error("Error fetching or updating genres:", error.message);
  }
}

/**
 * Delete record and its associated data from IndexedDB.
 * @param {string} imdbID - The IMDb ID of the media to delete.
 * @param {string} mediaType - The type of media ('movie' or 'tv').
 * @param {Object} db - The IndexedDB instance.
 */
async function deleteRecord(imdbID, mediaType, db) {
  try {
    // Determine which store to use based on mediaType
    const mediaStore = mediaType === 'tv' ? 'series' : 'movies';

    // Start a transaction for the appropriate stores
    const transaction = db.transaction([mediaStore, "links", "lists"], "readwrite");
    const mediaStoreObj = transaction.objectStore(mediaStore);
    const linksStore = transaction.objectStore("links");
    const listsStore = transaction.objectStore("lists");

    // Helper function to delete all entries for a specific store and IMDb ID
    const deleteEntriesByImdbID = async (store, storeName) => {
      return new Promise((resolve, reject) => {
        const index = store.index("imdbID"); // Assuming an index on imdbID exists
        const request = index.getAllKeys(imdbID);
        request.onsuccess = async () => {
          const keysToDelete = request.result;
          if (keysToDelete.length > 0) {
            for (const key of keysToDelete) {
              await new Promise((res, rej) => {
                const deleteRequest = store.delete(key);
                deleteRequest.onsuccess = res;
                deleteRequest.onerror = rej;
              });
            }
            console.log(`Deleted ${keysToDelete.length} entries from "${storeName}" for IMDb ID "${imdbID}".`);
          }
          resolve();
        };
        request.onerror = () => {
          console.error(`Failed to fetch entries from "${storeName}" for IMDb ID "${imdbID}".`);
          reject(request.error);
        };
      });
    };

    // Step 1: Delete the media item from the appropriate store
    await new Promise((resolve, reject) => {
      const request = mediaStoreObj.delete(imdbID);
      request.onsuccess = () => {
        console.log(`Deleted ${mediaType} with IMDb ID "${imdbID}" from the "${mediaStore}" store.`);
        resolve();
      };
      request.onerror = () => {
        console.error(`Failed to delete ${mediaType} with IMDb ID "${imdbID}" from the "${mediaStore}" store.`);
        reject(request.error);
      };
    });

    // Step 2: Delete all associated links and lists
    await Promise.all([
      deleteEntriesByImdbID(linksStore, "links"),
      deleteEntriesByImdbID(listsStore, "lists")
    ]);

    // Step 3: Delete associated data from type-specific stores
    if (mediaType === 'movie') {
      // Delete from movie-specific stores
      await deleteFromRelatedStores(db, imdbID, [
        "movie_details",
        "movie_credits",
        "movie_keywords"
      ]);
    } else if (mediaType === 'tv') {
      // Delete from series-specific stores
      await deleteFromRelatedStores(db, imdbID, [
        "series_details",
        "series_credits",
        "series_keywords"
      ]);
    }

    // Step 4: Remove the corresponding DOM element
    const mediaCard = document.querySelector(`.media-card[data-imdbid="${imdbID}"]`);
    if (mediaCard) {
      mediaCard.remove();
      console.log(`Removed ${mediaType} card for IMDb ID "${imdbID}" from the DOM.`);
    }

    // Step 5: Remove the media from the global cache
    removeFromMediaCache(imdbID);

    // Step 6: Update filteredRecords and UI
    filteredRecords = filteredRecords.filter(media => media.imdbID !== imdbID);

    updateActiveYears();
    initializeGenreFilter();
    initializeYearSlider();
    totalRecords -= 1;
    updateMatchCount();

    // Commit transaction
    transaction.oncomplete = () => {
      console.log(`Transaction completed: Deleted ${mediaType}, associated links, and lists for IMDb ID "${imdbID}".`);
    };
    transaction.onerror = () => {
      console.error(`Transaction failed: Could not delete ${mediaType}, links, and lists for IMDb ID "${imdbID}".`);
    };
  } catch (error) {
    console.error(`Error deleting ${mediaType} with IMDb ID "${imdbID}":`, error);
  }
}

/**
 * Helper function to delete data from related stores using the tmdbID
 * @param {Object} db - The IndexedDB instance
 * @param {string} imdbID - The IMDb ID of the media
 * @param {Array} storeNames - Array of store names to delete from
 */
async function deleteFromRelatedStores(db, imdbID, storeNames) {
  try {
    // First, get the tmdbID from the media cache
    const mediaItem = mediaCache.get(imdbID);
    if (!mediaItem || !mediaItem.tmdbID) {
      console.warn(`No tmdbID found for IMDb ID "${imdbID}". Skipping related stores deletion.`);
      return;
    }

    const tmdbID = mediaItem.tmdbID;

    // Check which stores exist in the database
    const existingStores = storeNames.filter(store =>
      db.objectStoreNames.contains(store)
    );

    if (existingStores.length === 0) {
      console.log(`No relevant stores found for deletion.`);
      return;
    }

    // Create a transaction for all existing stores
    const transaction = db.transaction(existingStores, "readwrite");

    // Process each store
    for (const storeName of existingStores) {
      const store = transaction.objectStore(storeName);

      // If the store has a primary key of tmdbID
      if (store.keyPath === "tmdbID") {
        await new Promise((resolve, reject) => {
          const request = store.delete(tmdbID);
          request.onsuccess = () => {
            console.log(`Deleted entry with tmdbID "${tmdbID}" from "${storeName}".`);
            resolve();
          };
          request.onerror = () => {
            console.error(`Failed to delete entry with tmdbID "${tmdbID}" from "${storeName}".`);
            reject(request.error);
          };
        });
      }
      // If the store has an index on tmdbID
      else if (store.indexNames.contains("tmdbID")) {
        await new Promise((resolve, reject) => {
          const index = store.index("tmdbID");
          const request = index.getAllKeys(tmdbID);

          request.onsuccess = async () => {
            const keysToDelete = request.result;
            if (keysToDelete.length > 0) {
              for (const key of keysToDelete) {
                await new Promise((res, rej) => {
                  const deleteRequest = store.delete(key);
                  deleteRequest.onsuccess = res;
                  deleteRequest.onerror = rej;
                });
              }
              console.log(`Deleted ${keysToDelete.length} entries from "${storeName}" for tmdbID "${tmdbID}".`);
            }
            resolve();
          };

          request.onerror = () => {
            console.error(`Failed to fetch entries from "${storeName}" for tmdbID "${tmdbID}".`);
            reject(request.error);
          };
        });
      }
    }

    return new Promise((resolve) => {
      transaction.oncomplete = () => {
        console.log(`Successfully deleted data from related stores for tmdbID "${tmdbID}".`);
        resolve();
      };
      transaction.onerror = (event) => {
        console.error(`Error deleting from related stores:`, event.target.error);
        resolve(); // Still resolve to continue with the main deletion process
      };
    });

  } catch (error) {
    console.error(`Error in deleteFromRelatedStores:`, error);
    // Don't throw, just log the error and continue with the main deletion process
  }
}

/**
 * DB: Load all listStates from indexDb into cache for the current user
 * with improved username separation and DOM updates
 */
async function loadListStates() {
  if (!userSettings?.username) {
    console.error('Cannot load list states: No active user');
    return;
  }

  try {
    const transaction = db.transaction(["lists"], "readonly");
    const listStore = transaction.objectStore("lists");
    const usernameIndex = listStore.index("username");

    return new Promise((resolve, reject) => {
      const request = usernameIndex.getAll(IDBKeyRange.only(userSettings.username));

      request.onsuccess = event => {
        const userLists = event.target.result || [];
        console.log(`Loading ${userLists.length} list entries for user ${userSettings.username}`);

        // Reset all list states first while preserving object references
        mediaCache.forEach(media => {
          if (!media.lists) media.lists = {};
          // Clear existing list states but preserve the object structure
          Object.keys(media.lists).forEach(key => {
            media.lists[key] = false;
          });
        });

        // Update media with list membership
        userLists.forEach(record => {
          const media = mediaCache.get(record.imdbID);
          if (media) {
            if (!media.lists) media.lists = {};
            media.lists[record.list] = true;
          }
        });

        // Update DOM for all rendered titles (restored from old code)
        updateListIconsInDOM();

        console.log('List states loaded and DOM updated successfully');
        resolve();
      };

      request.onerror = event => {
        const error = event.target.error;
        console.error('Error loading list states:', error);
        reject(error);
      };

      transaction.onerror = event => {
        console.error('Transaction error loading list states:', event.target.error);
        reject(event.target.error);
      };
    });
  } catch (error) {
    console.error('Failed to load list states:', error);
    throw error;
  }
}

/**
 * Update all list icons in the DOM to match current state
 * (Preserved from original code for DOM synchronization)
 */
function updateListIconsInDOM() {
  document.querySelectorAll('.media-card').forEach(card => {
    const imdbID = card.dataset.imdbid;
    if (!imdbID) return;

    const topRowIcons = card.querySelector('.top-row-icons');
    if (topRowIcons) {
      updateIconStates(topRowIcons, imdbID);
    }
  });
}

function getListConfig(listName) {
  // First try user-specific config
  const listConfig = listsMap[listName];

  if (!listConfig) {
    // For core lists, try to find the default template
    if (isCoreList(listName)) {
      // Access default templates through a different method if needed
      return {
        symbolId: iconListConfig[listName]?.symbolId || listName,
        label: iconListConfig[listName]?.label || listName,
        type: "core"
      };
    }

    console.warn(`List configuration for "${listName}" not found in listsMap`);
    // Fallback for custom lists
    return {
      symbolId: "list_custom",
      label: listName.charAt(0).toUpperCase() + listName.slice(1),
      type: "custom"
    };
  }

  return {
    symbolId: listConfig.symbolId || listName,
    label: listConfig.label || listName,
    type: listConfig.type || "core"
  };
}

/**
 * Retrieves user lists with optional type filtering
 * @param {string} username - The username to get lists for
 * @param {string|null} listType - Optional filter by list type ('core', 'custom', etc.)
 * @returns {Promise<Array>} - Promise resolving to array of list metadata
 */
async function getUserLists(username, listType = null) {
  const transaction = db.transaction(["lists_metadata"], "readonly");
  const store = transaction.objectStore("lists_metadata");

  let request;
  if (listType) {
    // Use the compound index for more efficient querying when filtering by type
    const index = store.index("username_type");
    request = index.getAll(IDBKeyRange.only([username, listType]));
  } else {
    // Use the username index when getting all lists
    const index = store.index("username");
    request = index.getAll(IDBKeyRange.only(username));
  }

  return new Promise((resolve, reject) => {
    request.onsuccess = () => {
      const lists = request.result || [];
      // Sort with core lists first, then custom lists alphabetically
      lists.sort((a, b) => {
        if (a.type === 'core' && b.type !== 'core') return -1;
        if (a.type !== 'core' && b.type === 'core') return 1;
        return a.label.localeCompare(b.label);
      });
      resolve(lists);
    };
    request.onerror = () => reject(request.error);
  });
}

async function createUserList(username, listName, description, isPublic = false) {
  // Validate the list name doesn't conflict with core lists
  if (isCoreList(listName)) {
    throw new Error(`Cannot create list with reserved name: ${listName}`);
  }

  const listId = generateListId();

  const newList = {
    username,
    type: 'custom',
    list: listId,
    label: listName,
    symbolId: 'list_custom', // Default icon for custom lists
    description: description || `My ${listName} list`,
    created: Date.now(),
    lastUpdated: Date.now(),
    isPublic,
    ratings: {},
    ratingsCount: 0,
    avgRating: 0,
    tags: []
  };

  const transaction = db.transaction(["lists_metadata"], "readwrite");
  const store = transaction.objectStore("lists_metadata");

  return new Promise((resolve, reject) => {
    const request = store.add(newList);
    request.onsuccess = () => {
      // Update listsMap directly with the new list
      listsMap[listId] = newList;
      resolve(newList);
    };
    request.onerror = () => reject(request.error);
  });
}

/**
 * Update a custom list's label and description.
 * @param {string} username
 * @param {string} listId (UUID)
 * @param {string} newLabel
 * @param {string} newDescription
 * @returns {Promise<void>}
 */
async function updateUserList(username, listId, newLabel, newDescription) {
  const transaction = db.transaction(["lists_metadata"], "readwrite");
  const store = transaction.objectStore("lists_metadata");
  const index = store.index("username_list");
  const getRequest = index.get([username, listId]);

  return new Promise((resolve, reject) => {
    getRequest.onsuccess = function (event) {
      const record = event.target.result;
      if (!record) {
        reject(new Error("List not found."));
        return;
      }
      record.label = newLabel;
      record.description = newDescription;
      record.lastUpdated = Date.now();
      const putRequest = store.put(record);
      putRequest.onsuccess = () => {
        // Update the listsMap cache with the updated record
        if (listsMap[listId]) {
          listsMap[listId] = record;
        }
        resolve();
      };
      putRequest.onerror = (err) => reject(err);
    };
    getRequest.onerror = (err) => reject(err);
  });
}

/**
 * Delete a user list and all its assignments (movies in that list) by listId (UUID).
 * @param {string} username - The user's username.
 * @param {string} listId - The internal list ID (UUID or core list string).
 * @param {string} type - The list type ('custom' or 'core'). Default: 'custom'
 * @returns {Promise<void>}
 */
async function deleteUserList(username, listId, type = 'custom') {
  if (!db) throw new Error("Database not open");
  
  // First, remove the list from activeLists if it's being used as a filter
  const activeListIndex = activeLists.findIndex(item => 
    item.name === listId && item.type === type
  );
  
  if (activeListIndex !== -1) {
    activeLists.splice(activeListIndex, 1);
    // Update filter button state since we modified activeLists
    const customListIcon = document.getElementById('customlist-filter-icon');
    updateFilterButtonState(customListIcon);
  }
  
  return new Promise((resolve, reject) => {
    // Start a transaction for both stores
    const transaction = db.transaction(["lists_metadata", "lists"], "readwrite");

    // 1. Delete the list metadata
    const metaStore = transaction.objectStore("lists_metadata");
    const metaKey = [username, type, listId];
    metaStore.delete(metaKey);

    // 2. Delete all assignments for this list (all movies in this list)
    const listsStore = transaction.objectStore("lists");
    // Use the username_list index for efficient lookup
    const index = listsStore.index("username_list");
    const request = index.getAllKeys([username, listId]);
    request.onsuccess = function() {
      const keys = request.result || [];
      keys.forEach(key => listsStore.delete(key));
    };
    request.onerror = function(event) {
      console.error("Error finding assignments for list:", event.target.error);
      // Still try to resolve after metadata deletion
    };

    transaction.oncomplete = () => {
      // After successful deletion, we should also reapply filters
      // since we modified activeLists
      if (activeListIndex !== -1) {
        applyFiltersAndSearch();
      }
      resolve();
    };
    transaction.onerror = event => reject(event.target.error);
    transaction.onabort = event => reject(event.target.error);
  });
}

/**
 * Update watched icon with symbol approach using data-state
 */
function updateWatchedIcon(state) {
  const button = document.getElementById("watched-filter-icon");
  let symbolId, tooltipText;

  if (state === "watched") {
    symbolId = "eye";
    tooltipText = "Unwatched";
  } else if (state === "unwatched") {
    symbolId = "eye_closed";
    tooltipText = "Watched & Unwatched";
  } else { // "none"
    symbolId = "eye";
    tooltipText = "Watched";
  }

  // Update data attribute to reflect current state
  button.dataset.state = state;

  button.innerHTML = `
    <svg class="icon-svg">
      <use href="#${symbolId}"></use>
    </svg>
    <span class="ttip-txt" data-tt-pos="bottom">${tooltipText}</span>
  `;

  if (state === "none") {
    button.classList.remove("active");
  } else {
    button.classList.add("active");
  }
}

/**
 * updateActiveListsForWatched(state):
 * Updates the global activeLists array. For state "watched" or "unwatched",
 * it pushes the corresponding filter (with type "core"). For "none", it leaves watched-related filters out.
 */
function updateActiveListsForWatched(state) {
  activeLists = activeLists.filter(item =>
    item.name !== "watched" && item.name !== "unwatched"
  );
  if (state === "watched") {
    activeLists.push({ name: "watched", type: "core" });
  } else if (state === "unwatched") {
    activeLists.push({ name: "unwatched", type: "core" });
  }
  // "none": do nothing, meaning no watched filter.
}

async function showSettingsPopup(onFirstProfileSavedCallback) {
  // --- Fetch ALL usernames for the profile dropdown ---
  let allUsernames = [];
  try {
    allUsernames = await getAllUsernamesFromDB();
  } catch (error) {
    console.error("Failed to fetch usernames for settings:", error);
    showNotification("❌ Could not load profiles.", false);
  }

  // --- Use pre-loaded settings for the CURRENT user ---
  const isFirstUser = allUsernames.length === 0;
  // console.log(`showSettingsPopup: isFirstUser = ${isFirstUser}`);

  // Always fetch fresh settings from the database for the current user
  let currentUserSettings = {};
  if (!isFirstUser) {
    try {
      currentUserSettings = await getCurrentUserSettingsFromDB(userSettings.username);
      if (!currentUserSettings) {
        currentUserSettings = { ...userSettings };
        console.warn("Could not fetch fresh settings from DB, using cached settings");
      }
    } catch (error) {
      console.error("Error fetching current user settings:", error);
      currentUserSettings = { ...userSettings };
    }
  }

  const currentUser = isFirstUser ? null : userSettings.username;

  // Store initial JustWatch config for potential cancellation
  initialJWConfig = {
    links: currentUserSettings.justwatch_links,
    type: currentUserSettings.justwatch_type,
    providers: [...(currentUserSettings.justwatch_providers || [])]
  };

  // --- HTML Generation ---
  const settingsContent = `
    <div class="popup-panel">
     ${isFirstUser ? '<p class="setup-instruction">Welcome! Please create a profile and add API keys to get started.</p>' : ''}
     <form id="settings-form">
        <!-- Section 1: Profile -->
        <div class="settings">
           <div class="profile-row">
               <div class="user-icon-circle">
                 <label for="user-icon-file" class="profile-icon">
                 <div id="user-icon-preview">
                   <svg class="icon-svg"><use href="#user_default"/></svg>
                 </div>
                 </label>
                 <input type="file" id="user-icon-file" accept="image/png,image/jpeg,image/svg+xml, image/webp, image/gif">
               </div>
               <div class="username-container"> 
               <button id="add-profile" class="ttip-button add-item" aria-label="+" type="button" title="Create new profile">+</button>
       <select id="profilename" class="dropdown-input medium-input">
                   ${isFirstUser
      ? '<option value="" disabled selected>Select Profile...</option>'
      : allUsernames.map(user => `<option value="${user}" ${user === currentUser ? "selected" : ""}>${user}</option>`).join('')
    }
                   <option value="__add_new__">Create Profile...</option>
                 </select>
       <button id="delete-profile" class="ttip-button delete-profile" ${isFirstUser || allUsernames.length <= 1 ? 'style="display: none;"' : ''} aria-label="Delete Profile" type="button" title="Delete Profile">
            <svg class="delete-icon"><use href="#trash"/></svg>
       </button>
               </div>
               <!-- Buttons -->
               <div class="popup-buttons">
                 <button type="button" id="settings-close" class="close-modal" data-popup-type="settings">×</button>
               </div>
           </div>
        </div>
        <span class="settings-separator" data-tooltip>
          <span class="separator-text">API Keys<span class="ttip-txt" data-tt-pos="bottom" style="padding:4px 12px;"><p>Visit <a href="${tmdbBaseUrl}settings/api/" target="_blank">TMDB</a> and <a href="https://www.omdbapi.com/apikey.aspx" target="_blank">OMDb</a> to get free API keys.</p><p>Enter your OMDB and TMDB API Keys below.</p></span></span>
        </span>
        <!-- Section 2: API Keys -->
        <div class="settings" style="display: flex; align-items: center; gap: 10px;">
          <!-- OMDB Group -->
          <div class="api-key-group" style="display: flex; align-items: center; gap: 5px;">
            <label for="omdb-key" class="label">OMDB</label>
            <button id="add-omdb-key" class="ttip-button add-item" aria-label="+" type="button" title="Add new OMDB API key">+</button>
            <select id="omdb-key" class="dropdown-input maxcontent-dropdown">
               <!-- Populated by loadUserSettingsDB -->
            </select>
            <button id="delete-key" class="ttip-button delete-key" aria-label="×" type="button" title="Delete selected OMDB API key">×</button>
          </div>
          <!-- TMDB Group -->
          <div class="api-key-group" style="display: flex; align-items: center; gap: 5px; flex: 1;">
            <label for="tmdb-key" class="label">TMDB</label>
            <input type="text" id="tmdb-key" required class="dropdown-input" style="flex: 1;" value=""> <!-- Populated by loadUserSettingsDB -->
          </div>
        </div>
        <span class="settings-separator" data-tooltip>
          <span class="separator-text">Backup Schedule<span class="ttip-txt" data-tt-pos="bottom"<p>Enable for periodic backup of the whole indexedDB as .json.</p>
          <p>File will be stored in the browser's download folder.</p></span></span>
        </span>
        <div class="settings">
          <div class="backup-row">
            <div class="backup-toggle">
              <label class="toggle-switch">
                <input type="checkbox" id="backup-active" class="toggle-input">
                <span class="toggle-label"></span>
                <span class="default-filename">${new Date().toISOString().slice(0, 19).replace('T', '_').replace(/:/g, '_')}_filmy-db-backup.json</span>
              </label>
            </div>
            <input type="datetime-local" id="backup-datetime" class="dropdown-input maxcontent-dropdown">
            <select id="backup-interval" class="dropdown-input maxcontent-dropdown">
              <option value="daily">Daily</option>
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
            </select>
          </div>
        </div>
        <span class="settings-separator" data-tooltip>
          <span class="separator-text"><svg class="justwatch-icon"><use href="#justwatch_icon" /></svg><span class="justwatch-icon">JustWatch (NYI)</span><span class="ttip-txt" data-tt-pos="bottom">Not Yet Implemented
          <p>Renders a simple JustWatch TMDB link for user region.<br/>
          Select your country and the grid will be populated by known providers.<br/>
          The Type and Links dropdowns are placeholders for future features.</p></span></span>
        </span>
        <div class="settings">
          <div class="justwatch-row">
            <label for="justwatch-country">Country</label>
            <select id="justwatch-country" class="dropdown-input maxcontent-dropdown">
               <!-- Populated by loadUserSettingsDB -->
            </select>
            <label for="justwatch-type">Type</label>
            <select id="justwatch-type" class="dropdown-input maxcontent-dropdown">
               <!-- Type options will be populated dynamically -->
            </select>
            <label for="justwatch-links">Links</label>
            <select id="justwatch-links" class="dropdown-input maxcontent-dropdown">
              <option value="all">All</option>
              <option value="stream">Stream</option>
              <option value="local">Local</option>
              <option value="web">Web</option>
            </select>
          </div>
          <div id="justwatch-providers-container" class="justwatch-row providers">
         <div id="justwatch-provider" class="providers-grid">
           ${isFirstUser ? '<small style="grid-column: 1 / -1;">Setup profile & API keys first</small>' : ''}
          </div>
        </div>
      </form>
    </div>
  `;

  // --- Display Popup ---
  showPopup('settings', settingsContent);

  // --- Populate Form Fields ---
  loadUserSettingsDB(currentUserSettings, isFirstUser);

  // --- Setup Event Listeners ---
  setupSettingsEventListeners(currentUserSettings, allUsernames, isFirstUser, onFirstProfileSavedCallback);
}

/**
 * Fetches all usernames (keys) from the user_settings store.
 * @returns {Promise<string[]>} - Array of usernames.
 */
async function getAllUsernamesFromDB() {
  if (!db) throw new Error("Database not open");

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(["user_settings"], "readonly");
    const store = transaction.objectStore("user_settings");
    const request = store.getAllKeys(); // Efficiently get all primary keys

    request.onsuccess = (event) => {
      resolve(event.target.result || []);
    };
    request.onerror = (event) => {
      reject(`Error fetching all usernames: ${event.target.error}`);
    };
  });
}

function loadUserSettingsDB(settings, isFirstUser = false) {
  // Ensure settings is an object, even if empty
  if (!settings || typeof settings !== 'object') {
    console.warn("loadUserSettingsDB called with invalid settings. Using empty object.", settings);
    settings = {};
  }

  // console.log("Loading settings into form. isFirstUser:", isFirstUser, "settings:", settings);

  const countryCode = Intl.DateTimeFormat().resolvedOptions().locale.split('-')[1] || 'US';

  // --- Safely Access DOM Elements ---
  const tmdbKeyInput = document.getElementById('tmdb-key');
  const backupDateInput = document.getElementById('backup-datetime');
  const backupIntervalSelect = document.getElementById('backup-interval');
  const backupActiveCheckbox = document.getElementById('backup-active');
  const jwCountrySelect = document.getElementById('justwatch-country');
  const jwTypeSelect = document.getElementById('justwatch-type');
  const jwLinksSelect = document.getElementById('justwatch-links');
  const userIconPreview = document.getElementById('user-icon-preview');
  const providersContainer = document.getElementById('justwatch-provider');

  // --- Populate General Fields ---
  if (tmdbKeyInput) {
    tmdbKeyInput.value = settings.tmdb_apikey || '';
  } else {
    console.warn("Element tmdb-key not found.");
  }

  if (backupDateInput) {
    backupDateInput.value = settings.backup_schedule || new Date().toISOString().slice(0, 16);
  } else {
    console.warn("Element backup-datetime not found.");
  }

  if (backupIntervalSelect) {
    backupIntervalSelect.value = settings.backup_interval || 'daily';
  } else {
    console.warn("Element backup-interval not found.");
  }

  if (backupActiveCheckbox) {
    backupActiveCheckbox.checked = settings.backup_active === true;
    // console.log("Setting backup_active checkbox to:", settings.backup_active, typeof settings.backup_active);
  } else {
    console.warn("Element backup-active not found.");
  }

  if (jwCountrySelect) {
    jwCountrySelect.value = settings.justwatch_country || countryCode;
  } else {
    console.warn("Element justwatch-country not found.");
  }

  if (jwTypeSelect) {
    jwTypeSelect.value = settings.justwatch_type || 'all';
  } else {
    console.warn("Element justwatch-type not found.");
  }

  if (jwLinksSelect) {
    jwLinksSelect.value = settings.justwatch_links || 'all';
  } else {
    console.warn("Element justwatch-links not found.");
  }

  // --- Populate OMDB Keys Dropdown ---
  refreshOmdbKeyDropdownDB(settings);

  // --- Populate User Icon ---
  if (userIconPreview) {
    if (settings.user_icon && typeof settings.user_icon === 'string') {
      userIconPreview.innerHTML = `<img src="${settings.user_icon}" alt="User Icon">`;
    } else {
      userIconPreview.innerHTML = `<svg class="icon-svg"><use href="#user_default"/></svg>`;
    }
  } else {
    console.warn("Element user-icon-preview not found.");
  }

  // --- Conditionally Populate JustWatch Providers ---
  const canPopulateProviders = !isFirstUser && settings.username && settings.tmdb_apikey;

  if (canPopulateProviders) {
    // console.log("Conditions met, calling populateProvidersDropdown from loadUserSettingsDB.");
    // Use browser locale as fallback if no country is set
    const countryToUse = settings.justwatch_country ||
      Intl.DateTimeFormat().resolvedOptions().locale.split("-")[1] ||
      "US";

    // Pass the settings object being loaded
    populateProvidersDropdown(settings, countryToUse)
      .catch(err => {
        console.error("Error populating providers during settings load:", err);
        if (providersContainer) {
          providersContainer.innerHTML = '<small style="grid-column: 1 / -1;">Error loading providers</small>';
        }
      });
  } else {
    // Set placeholder based on the reason for skipping
    let reason = isFirstUser ? "Initial setup" :
      !settings.username ? "No username" :
        !settings.tmdb_apikey ? "No TMDB key" :
          "Unknown";
    console.log("Skipping JustWatch provider population: " + reason);

    if (providersContainer) {
      providersContainer.innerHTML = `<small style="grid-column: 1 / -1;">${isFirstUser ? 'Setup profile & API keys first' : 'Requires TMDB Key'}</small>`;
    } else {
      console.warn("Element justwatch-provider not found for placeholder.");
    }
  }
}

function setupSettingsEventListeners(currentSettingsRef, allUsernames, isFirstUser, onFirstProfileSavedCallback) {
  const popupElement = document.getElementById('settings-form');
  if (!popupElement) {
    console.error("Settings form element not found for attaching listeners.");
    return;
  }

  let usernames = [...allUsernames]; // Local copy for managing dropdown state if needed

  // Define the delete profile handler as a named function for reuse
  const handleDeleteProfile = async (event, dynamicIsFirstUser = null) => {
    event.preventDefault();
    const profileSelect = document.getElementById('profilename');
    const profileToDelete = profileSelect?.value;

    // Use the dynamically passed isFirstUser value if provided, otherwise use the closure variable
    const effectiveIsFirstUser = dynamicIsFirstUser !== null ? dynamicIsFirstUser : isFirstUser;

    // Only allow deletion if not the first/only user
    if (profileToDelete && profileToDelete !== "__add_new__" && !effectiveIsFirstUser) {
      showCustomConfirm(`Delete profile "${profileToDelete}"? This cannot be undone.`, async () => {
        try {
          await deleteUserSettingFromDB(profileToDelete); // Delete from DB

          // Update local usernames list first (for immediate UI feedback)
          const index = usernames.indexOf(profileToDelete);
          if (index !== -1) {
            usernames.splice(index, 1);
          }

          // Refresh usernames from database to ensure accuracy
          usernames = await getAllUsernamesFromDB();

          // If we deleted the last user, handle cleanup and reload
          if (usernames.length === 0) {
            // Remove any active input fields and event listeners
            const activeInputs = document.querySelectorAll('.dropdown-input');
            activeInputs.forEach(input => {
              // Remove all event listeners by cloning and replacing
              const newInput = input.cloneNode(true);
              if (input.parentNode) {
                input.parentNode.replaceChild(newInput, input);
              }
            });

            // Clear any pending timeouts
            for (let i = 1; i < 10000; i++) {
              window.clearTimeout(i);
            }

            // Reset user settings
            userSettings = { requiresSetup: true };

            // Reload the page after a short delay to ensure cleanup is complete
            setTimeout(() => {
              window.location.reload();
            }, 100);
          } else {
            // Switch to first available user
            const nextUser = usernames[0];
            try {
              const nextUserSettings = await getCurrentUserSettingsFromDB(nextUser);
              if (nextUserSettings) {
                // Update global state
                Object.assign(currentSettingsRef, nextUserSettings);
                userSettings.username = nextUser;
                localStorage.setItem('lastUsername', nextUser);
                apiKeyManager.initialize(nextUserSettings);

                // Close the current settings popup
                const closeBtn = document.getElementById('settings-close');
                if (closeBtn) {
                  closeBtn.click();
                }

                // Show notification
                showNotification(`Switched to profile "${nextUser}"`, false);

                // Reopen settings popup after a short delay
                setTimeout(() => {
                  showSettingsPopup();
                }, 300);
              } else {
                // Failed to load next user, reload app
                window.location.reload();
              }
            } catch (error) {
              console.error("Error switching to next user after delete:", error);
              window.location.reload();
            }
          }
        } catch (error) {
          console.error("Error deleting profile:", error);
          showNotification(`❌ Failed to delete profile: ${error.message || error}`, false);
        }
      });
    }
  };

  // Use event delegation for the delete profile button with correct selector
  popupElement.addEventListener('click', (event) => {
    // Fix the selector - add the dot for class selector
  const deleteBtn = event.target.closest('#delete-profile');
    if (deleteBtn) {
      // Dynamically determine if this is the first/only user
      // console.log("Delete profile button clicked. Usernames length:", usernames.length);
      const dynamicIsFirstUser = usernames.length <= 1;

      // Call handleDeleteProfile with dynamic isFirstUser parameter
      handleDeleteProfile(event, dynamicIsFirstUser);
    }
  });

  const updateAndSaveSettings = async () => {
    // console.log("updateAndSaveSettings triggered....");

    // --- Explicitly Get Current Username from UI ---
    const profileSelectElement = document.getElementById('profilename');
    const currentProfileUsername = profileSelectElement ? profileSelectElement.value : null;

    if (!currentProfileUsername || currentProfileUsername === "__add_new__") {
      console.error("Cannot save settings: No valid profile selected in dropdown.");
      return; // Stop saving if no valid profile is selected
    }

    // console.log("Preparing to save settings for user: " + currentProfileUsername);

    // --- Read other form values ---
    const selectedProviders = Array.from(
      popupElement.querySelectorAll('#justwatch-provider .provider-pill.selected')
    ).map(pill => pill.dataset.provider);

    // --- Prepare the object TO BE SAVED ---
    // Start with a clean object containing only the essential key
    const settingsToSave = {
      username: currentProfileUsername // REQUIRED: Explicitly set the username from dropdown
    };

    // --- Read form values and add/overwrite them ---
    // Use the in-memory array instead of reading from the DOM
    settingsToSave.omdb_apikeys = currentSettingsRef.omdb_apikeys || [];
    settingsToSave.tmdb_apikey = popupElement.querySelector('#tmdb-key')?.value.trim();
    settingsToSave.backup_schedule = popupElement.querySelector('#backup-datetime')?.value;
    settingsToSave.backup_interval = popupElement.querySelector('#backup-interval')?.value || 'daily';
    settingsToSave.backup_active = popupElement.querySelector('#backup-active')?.checked || false;
    settingsToSave.justwatch_country = popupElement.querySelector('#justwatch-country')?.value || 'US';
    settingsToSave.justwatch_providers = selectedProviders;
    settingsToSave.justwatch_links = popupElement.querySelector('#justwatch-links')?.value || 'all';
    settingsToSave.justwatch_type = popupElement.querySelector('#justwatch-type')?.value || 'all';

    // Read the base64 src from the potentially existing img tag
    settingsToSave.user_icon = popupElement.querySelector('#user-icon-preview img')?.src || null;

    // --- End Prepare Object ---
    // console.log("Settings object prepared for saving:", settingsToSave);

    try {
      // --- Call Save Function with the prepared object ---
      const savedUser = await saveUserSettingsToDB(settingsToSave);
      // console.log("Settings auto-saved successfully for " + savedUser);

      // --- Update Global State AFTER successful save ---
      // Ensure the global appSettings reflects what was just saved
      Object.assign(currentSettingsRef, settingsToSave);
      Object.assign(userSettings, settingsToSave);
      userSettings.username = savedUser;
      userSettings.user_icon = settingsToSave.user_icon;

      // Update API Key Manager with new keys
      apiKeyManager.initialize(settingsToSave);

      updateUserIconDisplay(); // Update UI based on potentially changed icon

      // --- Check if we can complete initialization ---
      // Only proceed with initialization if we have all required elements
      if (isFirstUser &&
        typeof onFirstProfileSavedCallback === 'function' &&
        settingsToSave.username &&
        settingsToSave.tmdb_apikey &&
        settingsToSave.omdb_apikeys &&
        settingsToSave.omdb_apikeys.length > 0) {

        console.log("All required settings configured, completing initialization...");
        setTimeout(() => {
          onFirstProfileSavedCallback();
        }, 500); // Add delay to ensure UI updates complete first
      }

      // --- Trigger provider refresh if key might have changed ---
      const canPopulateNow = settingsToSave.username && settingsToSave.tmdb_apikey;
      if (canPopulateNow) {
        console.log("Settings saved.");
        // Use browser locale as fallback
        const countryToUse = settingsToSave.justwatch_country ||
          Intl.DateTimeFormat().resolvedOptions().locale.split("-")[1] ||
          "US";

        // Pass the newly saved object to populateProviders
        populateProvidersDropdown(settingsToSave, countryToUse)
          .catch(err => {
            console.error("Error populating providers after save:", err);
          });
      } else {
        console.log("Skipping provider refresh: conditions not met (user/key).");
        const providersDiv = document.getElementById('justwatch-provider');
        if (providersDiv) {
          providersDiv.innerHTML = '<small style="grid-column: 1 / -1;">Requires TMDB Key</small>';
        }
      }
    } catch (error) {
      console.error("Auto-save failed:", error);
      showNotification("❌ Failed to save settings.", false);
    }
  };

  // --- Profile Select Listener ---
  const profileSelect = document.getElementById('profilename');
  profileSelect?.addEventListener('change', async (event) => {
    const selectedValue = event.target.value;
    console.log("Profile dropdown changed to: " + selectedValue);

    if (selectedValue === "__add_new__") {
      // Pass the potentially undefined callback down
      handleAddProfileInput(event.target, currentSettingsRef, isFirstUser, onFirstProfileSavedCallback);
    } else {
      // Switch Profile logic
      await switchToProfile(selectedValue, profileSelect, currentSettingsRef);
    }
  });

  // --- Add Profile Button Listener ---
  const addProfileBtn = document.getElementById('add-profile');
  addProfileBtn?.addEventListener('click', (event) => {
    event.preventDefault();
    // Directly trigger the add profile input without going through the dropdown
    handleAddProfileInput(profileSelect, currentSettingsRef, isFirstUser, onFirstProfileSavedCallback);
  });

  // Extract the profile switching logic to a reusable function
  async function switchToProfile(selectedValue, profileSelect, currentSettingsRef) {
    console.log("Switching to profile: " + selectedValue);
    try {
      const newSettings = await getCurrentUserSettingsFromDB(selectedValue);
      if (newSettings) {
        // Update global state before loading UI
        Object.assign(currentSettingsRef, newSettings); // Update global appSettings reference
        userSettings.username = newSettings.username;
        localStorage.setItem('lastUsername', userSettings.username);

        // Load the newly fetched settings into the form
        // Pass false for isFirstUser, as we are switching to an existing user
        loadUserSettingsDB(currentSettingsRef, false);
        console.log("Switched to and loaded settings for " + selectedValue);
      } else {
        showNotification("❌ Could not load settings for " + selectedValue + ".", false);
        profileSelect.value = currentSettingsRef.username; // Revert dropdown
      }
    } catch (error) {
      showNotification("❌ Error switching profile: " + error.message || error, false);
      profileSelect.value = currentSettingsRef.username; // Revert dropdown
    }
  }

  // --- OMDB Select Listener ---
  const omdbSelect = document.getElementById('omdb-key');
  omdbSelect?.addEventListener('change', async (event) => {
    if (event.target.value === "__add_new__") {
      handleAddOmdbKeyInput(event.target, currentSettingsRef); // Pass reference
    } else {
      // Save immediately if an existing key is selected
      await updateAndSaveSettings();
    }
  });

  // Add click event listener to handle the case when "Add Key..." is already selected
  omdbSelect?.addEventListener('click', (event) => {
    if (event.target.value === "__add_new__") {
      handleAddOmdbKeyInput(event.target, currentSettingsRef);
    }
  });

  // Add the mobile-friendly "Add Key" button here
  const addOmdbKeyBtn = popupElement.querySelector('#add-omdb-key');
  addOmdbKeyBtn?.addEventListener('click', () => {
    const omdbSelect = document.getElementById('omdb-key');
    handleAddOmdbKeyInput(omdbSelect, currentSettingsRef);
  });

  // --- Listeners for Auto-Save Fields ---
  // TMDB Key (blur/enter)
  const tmdbInput = popupElement.querySelector('#tmdb-key');
  const handleTmdbSave = async (event) => {
    const newValue = event.target.value.trim();
    // Only save if value actually changed from what's in memory/settings obj
    if (currentSettingsRef.tmdb_apikey !== newValue) {
      await updateAndSaveSettings();
    }
  };
  tmdbInput?.addEventListener('blur', handleTmdbSave);
  tmdbInput?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      e.target.blur(); // Trigger blur event which saves
    }
  });

  // JW Country/Type change
  const jwCountry = popupElement.querySelector('#justwatch-country');
  const jwType = popupElement.querySelector('#justwatch-type');
  const handleJwDropdownChange = async () => {
    // updateAndSaveSettings reads values, saves, and triggers provider refresh inside if conditions met
    await updateAndSaveSettings();
  };
  jwCountry?.addEventListener('change', handleJwDropdownChange);
  jwType?.addEventListener('change', handleJwDropdownChange);

  // General Auto-Save Fields on change event
  const autoSaveSelectors = [
    '#backup-datetime',
    '#backup-interval',
    '#backup-active',
    '#justwatch-links' // Country/Type handled above
  ];
  autoSaveSelectors.forEach(selector => {
    const element = popupElement.querySelector(selector);
    element?.addEventListener('change', updateAndSaveSettings);
  });

  // --- Listener for User Icon File Change ---
  const userIconFile = popupElement.querySelector('#user-icon-file');
  userIconFile?.addEventListener('change', function (e) { // No need for async here
    const file = e.target.files[0];
    if (file) {
      if (file.size > 500000) {
        showNotification("File size must be less than 500KB", false);
        return;
      }

      const reader = new FileReader();
      reader.onload = async function (e) { // Make the onload async
        const base64Icon = e.target.result;
        const userIconPreview = popupElement.querySelector('#user-icon-preview');
        if (userIconPreview) {
          userIconPreview.innerHTML = `<img src="${base64Icon}" alt="User Icon">`;

          // Trigger save AFTER the image preview is updated.
          // updateAndSaveSettings will read the new src from the DOM.
          await updateAndSaveSettings();
        }
      };
      reader.onerror = () => {
        console.error("Error reading file for user icon.");
        showNotification("❌ Error reading icon file.", false);
      };
      reader.readAsDataURL(file);
    }
  });

  // --- Listener for JW provider pills (Delegation) ---
  const providersContainer = popupElement.querySelector('#justwatch-provider');
  if (providersContainer) {
    providersContainer.addEventListener('click', async (event) => {
      const pill = event.target.closest('.provider-pill');
      if (pill) {
        pill.classList.toggle('selected'); // Visual toggle
        await updateAndSaveSettings(); // Reads new state and saves
      }
    });
  }

  // --- Listeners for DELETE buttons ---
  // Delete OMDB Key
  const deleteOmdbBtn = popupElement.querySelector('.delete-key');
  deleteOmdbBtn?.addEventListener('click', async (event) => {
    event.preventDefault();
    const keyToDelete = omdbSelect?.value; // Use optional chaining
    if (keyToDelete && keyToDelete !== "__add_new__" && keyToDelete !== "") {
      // Get keys directly from the reference object
      const currentKeys = currentSettingsRef.omdb_apikeys || [];

      // Check if key actually exists before attempting removal/save
      if (currentKeys.includes(keyToDelete)) {
        // Update in-memory settings before saving
        currentSettingsRef.omdb_apikeys = currentKeys.filter(k => k !== keyToDelete);

        // Update the DOM dropdown first to reflect the change
        refreshOmdbKeyDropdownDB(currentSettingsRef);

        try {
          // Save the modified settings to database
          await updateAndSaveSettings();

          // Update the API key manager with the new settings
          apiKeyManager.initialize(currentSettingsRef);

          omdbSelect?.focus();
          console.log("Deleted OMDB key:", keyToDelete);
        } catch (error) {
          showNotification("❌ Failed to delete OMDB key: " + (error.message || error), false);

          // Revert the in-memory change if save failed
          currentSettingsRef.omdb_apikeys = currentKeys;
          refreshOmdbKeyDropdownDB(currentSettingsRef);
        }
      } else {
        console.warn(`Attempted to delete OMDB key ${keyToDelete} which was not found in settings.`);
      }
    } else {
      showNotification("No valid key selected to delete.", false);
    }
  });

  // --- Handle Add Profile Input ---
  function handleAddProfileInput(selectElement, settingsRef, isFirstUser, callback) {
    if (!db) {
      console.error("Database not available for profile creation");
      showNotification("❌ Database error. Please refresh the page.", false);
      return;
    }
    // Create an input field to replace the dropdown temporarily
    const parentContainer = selectElement.parentNode;
    const inputField = document.createElement('input');
    inputField.type = 'text';
    inputField.className = 'dropdown-input medium-input';
    inputField.placeholder = 'Enter profile name...';
    inputField.maxLength = 30;

    // Hide the select element and show the input
    selectElement.style.display = 'none';
    parentContainer.insertBefore(inputField, selectElement);
    inputField.focus();
    inputField.select();
    // On iOS Safari, sometimes a synthetic click helps:
    if (/iPad|iPhone|iPod/.test(navigator.userAgent)) {
      inputField.click();
    }

    // Handle input field events
    const handleProfileSave = async () => {
      const newProfileName = inputField.value.trim();

      // Safety check - if elements no longer exist in DOM, exit early
      if (!parentContainer || !document.body.contains(parentContainer) ||
        !inputField || !parentContainer.contains(inputField)) {
        console.log("DOM elements no longer available, aborting profile save");
        return;
      }

      if (!newProfileName) {
        // Empty input, revert back to dropdown
        if (parentContainer.contains(inputField)) {
          parentContainer.removeChild(inputField);
        }
        selectElement.style.display = '';
        selectElement.value = settingsRef.username || '';
        return;
      }

      // Check if username exists with fresh data from DB
      try {
        // Get fresh list of usernames from DB to ensure accuracy
        const currentUsernames = await getAllUsernamesFromDB();

        if (currentUsernames.includes(newProfileName)) {
          showNotification(`❌ Profile "${newProfileName}" already exists.`, false);
          if (parentContainer.contains(inputField)) {
            parentContainer.removeChild(inputField);
          }
          selectElement.style.display = '';
          selectElement.value = settingsRef.username || '';
          return;
        }

        // Create new settings object with the new username and all required fields
        const newSettings = {
          username: newProfileName,
          omdb_apikeys: [],
          tmdb_apikey: '',
          backup_schedule: new Date().toISOString().slice(0, 16),
          backup_interval: 'daily',
          backup_active: false,
          justwatch_country: Intl.DateTimeFormat().resolvedOptions().locale.split('-')[1] || 'US',
          justwatch_type: 'all',
          justwatch_links: 'all',
          justwatch_providers: [],
          user_icon: null,
          requiresSetup: false
        };

        // Save the new profile
        await saveUserSettingsToDB(newSettings);

        // Update global state
        Object.assign(settingsRef, newSettings);
        userSettings.username = newProfileName;
        localStorage.setItem('lastUsername', newProfileName);

        // Update local usernames list with fresh data
        usernames = await getAllUsernamesFromDB();

        // Safety check again before DOM manipulation
        if (!parentContainer || !document.body.contains(parentContainer) ||
          !inputField || !parentContainer.contains(inputField)) {
          console.log("DOM elements no longer available after save, skipping UI update");
          return;
        }

        // Update dropdown with fresh usernames
        selectElement.innerHTML = usernames.map(user =>
          `<option value="${user}" ${user === newProfileName ? "selected" : ""}>${user}</option>`
        ).join('') + '<option value="__add_new__">Create Profile...</option>';

        // Show delete button if it was hidden (first user case)
        const deleteBtn = document.getElementById('delete-profile');
        if (deleteBtn) {
          // Hide button if it's first user OR if there's only one profile
          if (isFirstUser || usernames.length <= 1) {
            deleteBtn.style.display = 'none';
          } else {
            deleteBtn.style.display = '';
          }
        }

        // Remove input field and show dropdown
        if (parentContainer.contains(inputField)) {
          parentContainer.removeChild(inputField);
        }
        selectElement.style.display = '';
        selectElement.value = newProfileName;

        // Load settings form with new profile
        loadUserSettingsDB(newSettings, false);

        // Initialize API Key Manager with the new settings
        apiKeyManager.initialize(newSettings);

        // Note: We don't call the callback here - it will be called when all required settings are present
        console.log("Profile created successfully. Please enter API keys to complete setup.");
        showNotification(`✅ Profile "${newProfileName}" created. Please enter your API keys to continue.`, false);
        if (isFirstUser && typeof callback === 'function') {
          callback();
        }
      } catch (error) {
        console.error("Error creating new profile:", error);
        showNotification(`❌ Failed to create profile: ${error.message || error}`, false);

        // Revert UI - with safety checks
        if (parentContainer && document.body.contains(parentContainer) &&
          inputField && parentContainer.contains(inputField)) {
          parentContainer.removeChild(inputField);
        }
        selectElement.style.display = '';
        selectElement.value = settingsRef.username || '';
      }
    };

    // Handle Enter key and blur events
    inputField.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        handleProfileSave();
      } else if (e.key === 'Escape') {
        // Cancel on Escape
        if (parentContainer.contains(inputField)) {
          parentContainer.removeChild(inputField);
        }
        selectElement.style.display = '';
        selectElement.value = settingsRef.username || '';
      }
    });

    inputField.addEventListener('blur', () => {
      // Small delay to allow for button clicks
      setTimeout(handleProfileSave, 300);
    });
  }

  function handleAddOmdbKeyInput(omdbSelectElement, currentSettingsRef) {
    const omdbKeySelect = document.getElementById("omdb-key");
    const selectWidth = window.getComputedStyle(omdbKeySelect).width;
    const parentContainer = omdbSelectElement.parentNode;
    const newInput = document.createElement("input");
    newInput.type = "text";
    newInput.className = "dropdown-input maxcontent-dropdown";
    newInput.placeholder = "Enter key";
    newInput.style.width = selectWidth;
    newInput.style.boxSizing = "border-box";
    parentContainer.insertBefore(newInput, omdbSelectElement.nextSibling);
    omdbSelectElement.style.display = "none";
    newInput.focus();

    const finalizeOmdbAdd = async () => {
      const newKey = newInput.value.trim();

      // Always restore the UI first
      omdbSelectElement.style.display = ""; // Show select
      newInput.remove(); // Remove input

      if (!newKey) {
        // If input was empty, just restore UI and return
        return;
      }

      // Ensure the keys array exists with the correct property name
      if (!Array.isArray(currentSettingsRef.omdb_apikeys)) {
        currentSettingsRef.omdb_apikeys = [];
      }

      // Add key if it doesn't exist
      if (!currentSettingsRef.omdb_apikeys.includes(newKey)) {
        // Add to the array in memory
        currentSettingsRef.omdb_apikeys.push(newKey);

        try {
          // Use updateAndSaveSettings instead of direct DB save
          await updateAndSaveSettings();

          // Update API Key Manager with new keys
          apiKeyManager.initialize(currentSettingsRef);

          // Update the dropdown UI
          refreshOmdbKeyDropdownDB(currentSettingsRef);

          // Select the newly added key
          omdbSelectElement.value = newKey;

          console.log(`Added OMDB key: ${newKey}`);
          showNotification("✓ OMDB API key added.", false);
        } catch (error) {
          console.error("Error adding OMDB key:", error);
          showNotification("❌ Failed to save new OMDB key.", false);

          // Remove the key from memory if save failed
          currentSettingsRef.omdb_apikeys = currentSettingsRef.omdb_apikeys.filter(k => k !== newKey);
          refreshOmdbKeyDropdownDB(currentSettingsRef);
        }
      } else {
        // Key already exists, just select it
        omdbSelectElement.value = newKey;
      }
    };

    newInput.addEventListener("blur", finalizeOmdbAdd);
    newInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        newInput.blur(); // Trigger the blur event which calls finalizeOmdbAdd
      } else if (e.key === "Escape") {
        omdbSelectElement.style.display = "";
        newInput.remove();
        // Revert to previous selection if possible
        const previousKey = currentSettingsRef.omdb_apikeys && currentSettingsRef.omdb_apikeys.length > 0 ?
          currentSettingsRef.omdb_apikeys[0] : "";
        omdbSelectElement.value = previousKey;
      }
    });
  }
}

function refreshOmdbKeyDropdownDB(settings) {
  const omdbKeyDropdown = document.getElementById('omdb-key');
  if (!omdbKeyDropdown) return; // Element might not be ready

  // Ensure we're working with an array
  const keys = Array.isArray(settings.omdb_apikeys) ? settings.omdb_apikeys : [];

  // Log for debugging
  // console.log("Refreshing OMDB key dropdown with keys:", keys);

  if (keys.length > 0) {
    // Build the dropdown options
    omdbKeyDropdown.innerHTML = keys
      .map(key => `<option value="${key}">${key}</option>`)
      .join('') +
      '<option value="__add_new__">Add Key...</option>';

    // Check if current selection still exists in the keys array
    const currentSelection = omdbKeyDropdown.value;
    if (currentSelection !== "__add_new__" && !keys.includes(currentSelection)) {
      // If the current selection was deleted, select the first available key
      omdbKeyDropdown.value = keys[0];
    }
  } else {
    omdbKeyDropdown.innerHTML = '<option value="" disabled selected>No keys added</option>' +
      '<option value="__add_new__">Add Key...</option>';

    // Force selection to add new when no keys are available
    omdbKeyDropdown.value = "__add_new__";
  }
}

// --- NEW DB Helper: Delete a user's settings ---
async function deleteUserSettingFromDB(username) {
  if (!db) throw new Error("Database not open");
  if (!username) throw new Error("Username required for deletion");

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(["user_settings"], "readwrite");
    const store = transaction.objectStore("user_settings");
    const request = store.delete(username);

    request.onsuccess = () => {
      console.log(`Settings deleted for user ${username}`);
      resolve();
    };
    request.onerror = (event) => {
      reject(`Error deleting settings for ${username}: ${event.target.error}`);
    };
  });
}

// --- Modify your existing getCurrentUserSettingsFromDB to accept username ---
async function getCurrentUserSettingsFromDB(username) { // Added username parameter
  if (!db) throw new Error("Database not open");
  if (!username) throw new Error("Username not set"); // Keep check

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(["user_settings"], "readonly");
    const store = transaction.objectStore("user_settings");
    const request = store.get(username); // Use passed username

    request.onsuccess = (event) => {
      resolve(event.target.result || null);
    };
    request.onerror = (event) => {
      reject(`Error fetching settings for ${username}: ${event.target.error}`);
    };
  });
}

async function loadMediaSettings() {
  if (!userSettings?.username) {
    console.error("Cannot load media settings: Username not set.");
    return;
  }

  try {
    const transaction = db.transaction(["media_settings"], "readonly");
    const store = transaction.objectStore("media_settings");
    const index = store.index("username"); // Use the new simple index

    return new Promise((resolve) => {
      const request = index.getAll(IDBKeyRange.only(userSettings.username));

      request.onsuccess = (event) => {
        const results = event.target.result || [];
        // Transform results into a cache object grouped by IMDb ID
        mediaSettingsCache = results.reduce((acc, setting) => {
          if (!acc[setting.imdbID]) acc[setting.imdbID] = {};
          acc[setting.imdbID][setting.type] = setting.value;
          return acc;
        }, {});

        console.log(`Loaded ${Object.keys(mediaSettingsCache).length} media settings.`);
        resolve(mediaSettingsCache);
      };

      request.onerror = (event) => {
        console.error("Error loading media settings:", event.target.error);
        resolve({});
      };
    });
  } catch (error) {
    console.error("Error initializing media settings cache:", error);
    return {};
  }
}

// Getter
function getMediaCustomization(imdbID, type) {
  return mediaSettingsCache[imdbID]?.[type] || null;
}

/**
 * Set a media customization (e.g., poster or backdrop) for a specific IMDb ID.
 * Updates both the global cache and IndexedDB.
 * @param {string} imdbID - IMDb ID of the media item.
 * @param {string} type - Type of customization ("poster", "backdrop").
 * @param {string} value - The value to set (e.g., URL or file path).
 */
function setMediaCustomization(imdbID, type, value) {
  if (!userSettings?.username) {
    console.error(`Cannot set ${type}: No active user.`);
    return;
  }

  // Update cache immediately
  mediaSettingsCache[imdbID] = mediaSettingsCache[imdbID] || {};
  mediaSettingsCache[imdbID][type] = value;

  // Trigger async DB update
  (async () => {
    try {
      const transaction = db.transaction(["media_settings"], "readwrite");
      const store = transaction.objectStore("media_settings");

      const record = {
        username: userSettings.username,
        imdbID,
        type,
        value,
        timestamp: new Date().toISOString()
      };

      await store.put(record);
      console.log(`Set ${type} for ${imdbID} in DB.`);
    } catch (error) {
      console.error(`Failed to update ${type} for ${imdbID} in DB:`, error);
    }
  })();
}

/**
 * Revert a media customization (e.g., poster or backdrop) for a specific IMDb ID.
 * Removes both from the global cache and IndexedDB.
 * @param {string} imdbID - IMDb ID of the media item.
 * @param {string} type - Type of customization ("poster", "backdrop").
 */
function clearMediaCustomization(imdbID, type) {
  if (!userSettings?.username) {
    console.error(`Cannot revert ${type}: No active user.`);
    return;
  }

  // Update cache immediately
  if (mediaSettingsCache[imdbID]) {
    delete mediaSettingsCache[imdbID][type];
    if (!Object.keys(mediaSettingsCache[imdbID]).length) {
      delete mediaSettingsCache[imdbID];
    }
  }

  // Trigger async DB removal
  (async () => {
    try {
      const transaction = db.transaction(["media_settings"], "readwrite");
      const store = transaction.objectStore("media_settings");
      const index = store.index("username_imdbID_type");
      const key = [userSettings.username, imdbID, type];
      const getKeyRequest = index.getKey(key);

      getKeyRequest.onsuccess = async function (event) {
        const id = event.target.result;
        if (id !== undefined) {
          await store.delete(id);
          console.log(`Cleared ${type} for ${imdbID} in DB.`);
        } else {
          console.log(`No ${type} customization found for ${imdbID} in DB.`);
        }
      };
      getKeyRequest.onerror = function (event) {
        console.error(`Failed to find ${type} for ${imdbID} in DB:`, event.target.error);
      };
    } catch (error) {
      console.error(`Failed to remove ${type} for ${imdbID} in DB:`, error);
    }
  })();
}

function updateProviderGrid(data, settings) {
  const countryDropdown = document.getElementById('justwatch-country');
  const typeDropdown = document.getElementById('justwatch-type');
  const providersContainer = document.getElementById('justwatch-provider');

  if (!countryDropdown || !typeDropdown || !providersContainer) {
    console.error("Required DOM elements are missing in updateProviderGrid.");
    return;
  }

  const selectedCountry = countryDropdown.value;
  // const selectedType = typeDropdown.value; // Always "all" until we can get the types via api

  // Filter providers that have a display priority for the selected region
  const filteredProviders = data.filter(provider =>
    provider.display_priorities &&
    Object.prototype.hasOwnProperty.call(provider.display_priorities, selectedCountry)
  );

  // Deduplicate providers by provider_name
  const uniqueProvidersMap = new Map();
  filteredProviders.forEach(provider => {
    if (!uniqueProvidersMap.has(provider.provider_name)) {
      uniqueProvidersMap.set(provider.provider_name, provider);
    }
  });

  // Get all unique providers as an array
  const allProviders = Array.from(uniqueProvidersMap.values());

  // Get the saved provider list or empty array if none
  const savedProviders = settings.justwatch_providers || [];

  // Separate selected and unselected providers
  const selectedProviders = [];
  const unselectedProviders = [];

  // First, collect all selected providers in their saved order
  savedProviders.forEach(providerName => {
    const providerObject = allProviders.find(p => p.provider_name === providerName);
    if (providerObject) {
      selectedProviders.push(providerObject);
    }
  });

  // Then collect all unselected providers
  allProviders.forEach(provider => {
    if (!savedProviders.includes(provider.provider_name)) {
      unselectedProviders.push(provider);
    }
  });

  // Sort unselected providers alphabetically
  unselectedProviders.sort((a, b) => a.provider_name.localeCompare(b.provider_name));
  selectedProviders.sort((a, b) => a.provider_name.localeCompare(b.provider_name));

  // Combine the arrays - selected providers first, then unselected
  const sortedProviders = [...selectedProviders, ...unselectedProviders];

  // Clear the providers container
  providersContainer.innerHTML = '';

  // Display providers with selected ones first
  const tmdbLogoBaseUrl = tmdbImgBaseUrl + "w200"; // Uses existing global variable

  sortedProviders.forEach(provider => {
    if (provider.logo_path) {
      const pill = document.createElement('div');
      pill.classList.add('provider-pill');
      pill.dataset.provider = provider.provider_name;
      pill.dataset.providerId = provider.provider_id; // Store ID for reference

      const img = document.createElement('img');
      img.src = tmdbLogoBaseUrl + provider.logo_path;
      img.alt = provider.provider_name;
      img.title = provider.provider_name;
      pill.appendChild(img);

      // Mark provider as selected if it exists in settings
      if (savedProviders.includes(provider.provider_name)) {
        pill.classList.add('selected');
      }

      // Add click event listener directly to the pill
      pill.addEventListener('click', () => {
        const isSelected = pill.classList.toggle('selected');

        if (!settings.justwatch_providers) {
          settings.justwatch_providers = [];
        }

        // Update settings
        if (isSelected) {
          // Add to the beginning of the array if not already present
          if (!settings.justwatch_providers.includes(provider.provider_name)) {
            settings.justwatch_providers.unshift(provider.provider_name);
          }
        } else {
          // Remove from array
          settings.justwatch_providers = settings.justwatch_providers.filter(
            name => name !== provider.provider_name
          );
        }

        // Save settings and refresh the grid
        saveUserSettingsToDB(settings);
        updateProviderGrid(data, settings);
      });

      providersContainer.appendChild(pill);
    }
  });
}

async function populateProvidersDropdown(existingSettings, existingCountryCode) {
  const countryDropdown = document.getElementById("justwatch-country");
  const typeDropdown = document.getElementById("justwatch-type");
  const providersContainer = document.getElementById("justwatch-provider");

  if (!countryDropdown || !typeDropdown || !providersContainer) {
    console.error("Required DOM elements are missing.");
    return;
  }

  // Only fall back to locale if explicitly no saved country code
  const userCountry = existingSettings?.justwatch_country || existingCountryCode;

  // Browser locale as fallback only when no saved setting exists
  const browserLocaleCountry = Intl.DateTimeFormat().resolvedOptions().locale.split("-")[1] || "US";

  // Prioritize user setting, fall back to browser locale only if needed
  const selectedCountry = userCountry || browserLocaleCountry;

  // console.log(`Loading providers for country: ${selectedCountry} (saved: ${userCountry}, browser: ${browserLocaleCountry})`);

  const settings = existingSettings || userSettings;

  // Ensure a TMDB API key has been set
  const tmdbApiKey = apiKeyManager.getTmdbKey();
  if (!tmdbApiKey) {
    console.error("TMDB API key is not provided.");
    return;
  }

  // Get providers data from IndexedDB or fetch fresh data if stale
  let providersData = await getProvidersData();
  const data = providersData.results || [];

  // Populate country dropdown: build a set of region codes from display priorities
  const regionCodesSet = new Set();
  data.forEach(provider => {
    if (provider.display_priorities) {
      Object.keys(provider.display_priorities).forEach(region => regionCodesSet.add(region));
    }
  });
  const regionCodes = Array.from(regionCodesSet).sort((a, b) => a.localeCompare(b));

  const currentSelection = selectedCountry;

  countryDropdown.innerHTML = "";
  regionCodes.forEach(regionCode => {
    const option = document.createElement("option");
    option.value = regionCode;
    option.textContent = regionCode;
    countryDropdown.appendChild(option);
  });

  countryDropdown.value = currentSelection;
  // console.log(`Set country dropdown to: ${countryDropdown.value} (expected: ${currentSelection})`);
    
  // Populate type dropdown (simplified to only "all")
  typeDropdown.innerHTML = "";
  ["all"].forEach(typeValue => {
    const option = document.createElement("option");
    option.value = typeValue;
    option.textContent = typeValue === "all" ? "All" : typeValue;
    typeDropdown.appendChild(option);
  });
  typeDropdown.value = settings.justwatch_type || "all";

  updateProviderGrid(data, settings);
}


// Helper function to check whether stored data is stale
function isDataStale(timestamp, maxAgeDays) {
  const now = new Date();
  const storedTime = new Date(timestamp);
  const daysDifference = (now - storedTime) / (1000 * 60 * 60 * 24);
  return daysDifference > maxAgeDays;
}

// Function to fetch providers data using IndexedDB as a cache
async function getProvidersData(maxAgeDays = 7) {
  try {
    // Try to get the cached data from IndexedDB
    const transaction = db.transaction(['provider_data'], 'readonly');
    const store = transaction.objectStore('provider_data');
    const cached = await new Promise((resolve, reject) => {
      const request = store.get('providers');
      request.onsuccess = () => resolve(request.result);
      request.onerror = (e) => reject(e.target.error);
    });

    // Use cached data if it exists and isn't stale
    if (cached && cached.timestamp && !isDataStale(cached.timestamp, maxAgeDays)) {
      // console.log("Using cached provider data");
      return cached.data;
    }

    // Fetch fresh data from TMDB API
    console.log("Fetching fresh provider data from TMDB");
    const providersUrl = `https://api.themoviedb.org/3/watch/providers/movie?api_key=${apiKeyManager.getTmdbKey()}&language=en-US`;
    const freshData = await queryTMDBSimple(providersUrl);

    if (freshData && freshData.results && freshData.results.length > 0) {
      // Store the fresh data in IndexedDB
      const writeTx = db.transaction(['provider_data'], 'readwrite');
      const writeStore = writeTx.objectStore('provider_data');
      await new Promise((resolve, reject) => {
        const request = writeStore.put({
          id: 'providers',
          data: freshData,
          timestamp: new Date().toISOString()
        });
        request.onsuccess = () => resolve();
        request.onerror = (e) => reject(e.target.error);
        writeTx.oncomplete = () => resolve();
      });

      console.log("Provider data updated in IndexedDB");
      return freshData;
    } else {
      console.warn("Received empty data from API, keeping existing data");
      return cached ? cached.data : { results: [] };
    }
  } catch (error) {
    console.error("Error fetching providers data:", error);

    // Try to get any cached data even if it's stale
    try {
      const fallbackTx = db.transaction(['provider_data'], 'readonly');
      const fallbackStore = fallbackTx.objectStore('provider_data');
      const fallbackData = await new Promise((resolve, reject) => {
        const request = fallbackStore.get('providers');
        request.onsuccess = () => resolve(request.result);
        request.onerror = (e) => reject(e.target.error);
      });

      return fallbackData ? fallbackData.data : { results: [] };
    } catch (fallbackError) {
      console.error("Could not retrieve fallback data:", fallbackError);
      return { results: [] };
    }
  }
}

/**
 * Extract and parse a numeric year from various year formats.
 * Handles formats like "2018", "2018–2024", "2018-2024", etc.
 * 
 * @param {string} yearStr - Year string to parse
 * @returns {number|null} - Parsed numeric year or null if invalid
 */
function parseYear(yearStr) {
  if (!yearStr) return null;

  // Use regex to extract the first 4-digit year in the string
  const match = yearStr.match(/(\d{4})/);
  return match ? parseInt(match[1], 10) : null;
}

/**
 * Convert all media items' year values to numeric format.
 * For movies: uses Year property
 * For series: uses first_air_date or Year property
 * 
 * @param {Array} media - Array of media objects
 * @returns {Array} - Same array with numericYear added to each item
 */
function convertYearToNumber(media) {
  // Check if media is not an array (likely a single object)
  if (!Array.isArray(media)) {
    media = [media]; // Convert to array with one element
  }

  return media.map(item => {
    let numericYear = null;

    if (item.mediaType === 'tv') {
      // For TV series, try first_air_date first, then fall back to Year
      if (item.first_air_date) {
        numericYear = parseYear(item.first_air_date);
      } else if (item.Year) {
        numericYear = parseYear(item.Year);
      }
    } else {
      // For movies, use Year property
      numericYear = parseYear(item.Year);
    }

    return {
      ...item,
      numericYear
    };
  });
}

/**
 * Populate Country dropdown with "origin_country" code from "movie_details" store
 * @param {*} movieDetails 
 */
function populateCountryDropdown(movieDetails) {
  const dropdownMenu = document.getElementById("dropdown-menu-country");
  const uniqueCountries = new Set();

  movieDetails.forEach(detail => {
    if (Array.isArray(detail.origin_country)) {
      detail.origin_country.forEach(code => {
        uniqueCountries.add(code.trim());
      });
    } else if (typeof detail.origin_country === 'string') {
      detail.origin_country.split(',').forEach(code => {
        uniqueCountries.add(code.trim());
      });
    }
  });

  // Convert the set to an array and sort alphabetically
  const sortedCountries = Array.from(uniqueCountries).sort();

  // Clear existing items and add a default "All" option
  dropdownMenu.innerHTML = '<li data-value="All" class="dropdown-item-country active all">All</li>';

  sortedCountries.forEach(code => {
    const li = document.createElement("li");
    li.classList.add("dropdown-item-country");
    li.setAttribute("data-value", code);
    li.textContent = code;
    dropdownMenu.appendChild(li);
  });
}

/**
 * Generates HTML for financial information display
 * @param {Object} mediaData - The media data containing budget and revenue
 * @returns {string} HTML for financial information
 */
function generateFinancialsHTML(mediaData) {
  // Skip if this isn't a movie or doesn't have financial data
  if (!mediaData || mediaData.mediaType === 'tv') {
    return '';
  }

  function getPerformanceData(budget, revenue) {
    if ((!budget || budget <= 0) && (!revenue || revenue <= 0)) {
      return { class: 'performance-neutral', label: 'Financials not available' };
    }
    if (!budget || budget <= 0) {
      return { class: 'performance-neutral', label: 'Budget not available' };
    }
    if (!revenue || revenue <= 0) {
      return { class: 'performance-neutral', label: 'Revenue not available' };
    }

    // Calculate the ratio
    const ratio = revenue / budget;

    // Determine performance level based on ratio
    if (ratio < 1.5) {
      return { class: 'performance-flop', label: 'Flop' };
    } else if (ratio < 2) {
      return { class: 'performance-breakeven', label: 'Break-even' };
    } else if (ratio < 3) {
      return { class: 'performance-success', label: 'Success' };
    } else if (ratio < 5) {
      return { class: 'performance-blockbuster', label: 'Blockbuster' };
    } else {
      return { class: 'performance-megahit', label: 'Mega-hit' };
    }
  }

  // Handle winner/loser layout
  let financialsClass = '';
  let additionalBudgetStyle = '';
  let additionalRevenueStyle = '';
  let performanceData = getPerformanceData(mediaData.budget, mediaData.revenue);

  if (mediaData.budget > mediaData.revenue) {
    financialsClass = ' budget-winner';
    additionalRevenueStyle = ' style="padding-left: 12px;"';
  } else if (mediaData.revenue > mediaData.budget) {
    financialsClass = ' revenue-winner';
    additionalBudgetStyle = ' style="padding-right: 12px;"';
  }

  // Create HTML with neutral styling for "N/A"
  const budgetHTML = mediaData.budget
    ? `<span class="budget"${additionalBudgetStyle}><strong>B</strong> ${formatAbbreviatedCurrency(mediaData.budget)}</span>`
    : `<span class="budget performance-neutral"><strong>B</strong>&nbsp;$ N/A</span>`;

  const revenueHTML = mediaData.revenue
    ? `<span class="revenue ${performanceData.class}"${additionalRevenueStyle}>
      <strong>R</strong> ${formatAbbreviatedCurrency(mediaData.revenue)}</span>`
    : `<span class="revenue performance-neutral"><strong>R</strong>&nbsp;$ N/A</span>`;

  return `
    <div class="financials${financialsClass}" data-tooltip>
      ${budgetHTML}
      ${revenueHTML}
      <span class="ttip-txt" data-tt-pos="bottom" style="font-size:1em;">${performanceData.label}</span>
    </div>
  `;
}

function generateCompaniesAndNetworksHTML(mediaData) {
  const includeWithoutLogo = false;
  const mediaType = mediaData.mediaType || (mediaData.first_air_date ? 'tv' : 'movie');

  // Filter entries based on media type
  let filteredEntries = [];
  if (mediaType === 'tv') {
    // For TV shows, only include networks
    filteredEntries = (mediaData.networks || []).map(network => ({
      ...network,
      type: 'network'
    }));
  } else {
    // For movies, only include production companies
    filteredEntries = (mediaData.production_companies || []).map(company => ({
      ...company,
      type: 'production'
    }));
  }

  // Skip the rest of processing if no entries
  if (filteredEntries.length === 0) return "";

  // Filter out similar names - keep shorter versions
  const uniqueEntries = {};

  // First pass - add all entries to the dictionary
  filteredEntries.forEach(entry => {
    if (!uniqueEntries[entry.name]) {
      uniqueEntries[entry.name] = entry;
    }
  });

  // Second pass - filter out longer names that contain shorter ones
  const entryNames = Object.keys(uniqueEntries);

  for (let i = 0; i < entryNames.length; i++) {
    const name1 = entryNames[i];
    if (!uniqueEntries[name1]) continue; // Already removed

    for (let j = 0; j < entryNames.length; j++) {
      if (i === j) continue;

      const name2 = entryNames[j];
      if (!uniqueEntries[name2]) continue; // Already removed

      // If one name is contained within the other
      if (name1 !== name2) {
        if (name1.includes(name2) && name2.length < name1.length) {
          // name2 is shorter and contained in name1, keep name2
          delete uniqueEntries[name1];
          break;
        } else if (name2.includes(name1) && name1.length < name2.length) {
          // name1 is shorter and contained in name2, keep name1
          delete uniqueEntries[name2];
        }
      }
    }
  }

  // Convert to array and filter/sort
  const finalEntriesList = Object.values(uniqueEntries)
    .filter(entry => includeWithoutLogo || entry.logo_path)
    .sort((a, b) => a.name.localeCompare(b.name));

  if (finalEntriesList.length === 0) return "";

  // Known COLOR logos that need grayscale instead of invert
  const colorLogos = {
    production: [
      174, // Warner Bros. Pictures
      1256, // PSO
      3756, // CoMix
    ],
    network: [
      // Add color network logos here
    ]
  };

  // Generate HTML for unique entries
  const entriesHTML = finalEntriesList.map(entry => {
    const typeClass = entry.type;
    const logoPath = entry.logo_path;

    // Determine the correct list to check based on entry type
    const listToCheck = entry.type === 'network' ? colorLogos.network : colorLogos.production;

    // Determine if the logo is color (needs grayscale) or monochrome (needs invert)
    const isColor = listToCheck.includes(entry.id);
    const colorClass = isColor ? 'color' : 'mono';

    // Build the logo class
    const logoClass = `${entry.file_type === ".svg" ? "svg-logo" : ""} company-logo ${colorClass}`;

    // If SVG is available, change the file extension in the path
    const imagePath = entry.file_type === ".svg"
      ? `${tmdbImgBaseUrl}w185${logoPath.replace(/\.png$/, '.svg')}`
      : `${tmdbImgBaseUrl}w185${logoPath}`;

    return `
      <div class="${typeClass}" data-id="${entry.id}" data-type="${entry.type}">
        ${logoPath
        ? `<img src="${imagePath}" alt="${entry.name}" class="${logoClass}">`
        : `<span class="company-name">${entry.name}</span>`
      }
      </div>
    `;
  }).join('');

  // Return the combined container
  return `
    <div class="production-companies">
      <div class="companies-list">
        ${entriesHTML}
      </div>
    </div>
  `;
}

/**
 * UI/DB/Cache: Detail View Functions
 * @param {*} mediaData 
 * @param {*} credits 
 * @param {*} peopleMap 
 * @param {*} keywords 
 * @returns 
 */
function generateDetailViewHtml(mediaData, credits, peopleMap, keywords) {

  // Determine media type - default to 'movie' if not specified
  const mediaType = mediaData.mediaType || 'movie';
  const isMovie = mediaType === 'movie';
  const isSeries = mediaType === 'tv';

  const defaultPoster = mediaData.defaultPoster || mediaData.Poster || createPlaceholderSVG(500, 750, 'poster');
  const activePoster = getMediaCustomization(mediaData.imdbID, 'poster');
  const posterPath = activePoster || defaultPoster;

  // const posterSrc = buildTMDbImageUrl(posterPath, "poster", "w500");
  // omdb lowres posters legacy support... remove later ###
  let posterSrc;
  if (posterPath.startsWith("https://m.media-amazon.com/")) {
    posterSrc = posterPath;
  } else {
    posterSrc = buildTMDbImageUrl(posterPath, "poster", "w500");
  }

  const showToggleButton = !!activePoster;

  const arrayToPillHtml = (
    array,
    { basePath = 'genre', nameKey = 'name', idKey = 'id', cssClass = 'genre', mediaType = 'movie' } = {}
  ) => {
    if (!Array.isArray(array) || array.length === 0) return 'N/A';
  
    return array
      .sort((a, b) => (a[nameKey] || '').localeCompare(b[nameKey] || '')) // Added null checks
      .map(item => {
        // Handle missing name or ID
        const name = item[nameKey] || 'Unknown';
        const itemId = item[idKey] || '';
        
        let url;
        if (basePath === 'network') {
          url = `${tmdbBaseUrl}network/${itemId}`;
        } else {
          const slug = `${itemId}-${name.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
          url = `${tmdbBaseUrl}${basePath}/${slug}/${mediaType}`;
        }
        
        return `<a href="${url}" class="${cssClass}" target="_blank">${name}</a>`;
      })
      .join(' ');
  };

  // Helper: Convert an array to a comma‑separated string.
  const arrayToCommaSeparatedString = (array) => {
    if (Array.isArray(array) && array.length > 0) {
      return array.sort((a, b) => a.localeCompare(b)).join(", ");
    }
    return "N/A";
  };

  // Custom Lists in Detail View
  const customListsHtml = generateCustomListsHtml(mediaData, customListsMeta);  

  const customListIcon = `<div class="icon-container customlist-icon" data-tooltip>   
          <svg class="icon-svg"><use href="#list_custom"/></svg>
                <span class="ttip-txt lists" data-tt-pos="bottom">${customListsHtml}</span>
  </div>  
      `;

  // Generate the icon element using your global function
  const topRowIcons = createTopRowIcons(mediaData.imdbID);
  if (topRowIcons) {
    // Update its state based on the current movie data
    updateIconStates(topRowIcons, mediaData.imdbID);
    topRowIcons.setAttribute('data-imdbid', mediaData.imdbID);
    topRowIcons.querySelectorAll('.icon-container').forEach(icon => {
      icon.setAttribute('data-imdbid', mediaData.imdbID);
    });
    topRowIcons.querySelectorAll('.ttip-txt').forEach(tooltip => {
      tooltip.dataset.ttPos = "bottom";
    });
  } else {
    console.error("Could not create top row icons for movie:", mediaData.imdbID);
  }
    const setIcon = (mediaData.belongs_to_collection && mediaData.belongs_to_collection.id)
    ? `<span class="set-icon" title="Part of a set"
      data-tmdb-url="${tmdbBaseUrl}collection/${mediaData.belongs_to_collection.id}">
  ${svgSet}
</span>`
    : "";

  const collectionHTML = mediaData.collection && mediaData.collection.length ? `
  <div class="details-row">
    <div class="header">${setIcon}${mediaData.belongs_to_collection.name} (${mediaData.collection.length})</div>
    <div class="card-container media">
      <!-- First card is the collection poster with link -->
<div class="card media collection-card" title="${mediaData.belongs_to_collection.name}"
     data-tmdb-url="${tmdbBaseUrl}collection/${mediaData.belongs_to_collection.id}">
        ${mediaData.belongs_to_collection.poster_path
      ? `<img src="${tmdbImgBaseUrl}w500${mediaData.belongs_to_collection.poster_path}" loading="lazy" alt="${mediaData.belongs_to_collection.name}" class="media-image">`
      : createPlaceholderSVG(185, 278, 'poster')}
        <div class="card-title">${mediaData.belongs_to_collection.name}</div>
      </div>
      <!-- Rest of collection cards -->
      ${generateMediaCards(mediaData.collection, false)}
    </div>
  </div>` : '';

  const ratingsHTML = generateRatingsHtml(mediaData);

  const awardWin = mediaData.Awards && mediaData.Awards.toLowerCase().includes("win");

  const awardsIcon = mediaData.Awards && mediaData.Awards !== "N/A" ? `<span class="award-icon ${awardWin ? 'award-win' : ''}" data-tooltip>${svgAward}<!--span class="ttip-txt" data-tt-pos="bottom">${mediaData.Awards.replace(/(award)(\d)| total$/gi, (_, g1, g2) => g1 ? `${g1}. ${g2}` : '')}</span-->${mediaData.Awards.replace(/(award)(\d)| total$/gi, (_, g1, g2) => g1 ? `${g1}. ${g2}` : '')}</span>` : "";

  const country = (Array.isArray(mediaData.Country) && mediaData.Country.length > 0)
    ? mediaData.Country
      .map(c => (["United States of America", "USA"].includes(c) ? "United States" : c))
      .sort((a, b) => a.localeCompare(b))
      .join(", ")
    : "N/A";

  const languages = arrayToCommaSeparatedString(mediaData.Language);

  const originalTitleHTML = (mediaData.original_title && mediaData.original_title !== mediaData.Title)
    ? `<div class="original-title">Original Title: ${mediaData.original_title}</div>`
    : "";

  const genres = Array.isArray(mediaData.genres)
  ? arrayToPillHtml(mediaData.genres, { basePath: 'genre', cssClass: 'genre', mediaType })
  : "N/A";

  const companiesAndNetworksHTML = generateCompaniesAndNetworksHTML(mediaData);

  // Financial information - only for movies
  const financialsHTML = isMovie ? generateFinancialsHTML(mediaData) : "";

  const taglineHTML = mediaData.tagline ? `<div class="tagline">${mediaData.tagline}</div>` : "";

  // const plotHTML = generatePlotHTML(mediaData); <div class="plot">${finalDescription}</div>
  // const trimmedPlot = mediaData.Plot ? mediaData.Plot.trim() : "";
  // const trimmedOverview = mediaData.overview ? mediaData.overview.trim() : "";
  const plotHTML = mediaData.overview ? `<div class="plot">${mediaData.overview}</div>` : ""; // tmdb
  // const plotHTML = `<div class="plot">${mediaData.Plot}</div>`; // omdb

  // TV show specific constants
  const number_of_seasons = mediaData.number_of_seasons || 0;
  // const number_of_episodes = mediaData.number_of_episodes || 0; // ### unused
  const seasons = mediaData.seasons || [];
  const status = mediaData.status || "N/A";
  const lastAirDate = mediaData.last_air_date !== "N/A"
    ? new Date(mediaData.last_air_date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
    : "N/A";

  const nextAirDate = mediaData.next_episode_to_air?.air_date
    ? new Date(mediaData.next_episode_to_air.air_date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
    : "";

  let statusText = "";
  if (status === "Returning Series") {
    statusText = nextAirDate ? `Next Episode on ${nextAirDate}` : "Returning Series";
  } else if (status === "Ended") {
    statusText = `Ended on ${lastAirDate}`;
  } else {
    statusText = status;
  }

  const tvStatusHTML = isSeries ? `<span class="bullet"></span>${number_of_seasons} Season${number_of_seasons !== 1 ? 's' : ''}<span class="bullet"></span>${statusText}` : "";

  // Date formatting utility functions (moved to this scope)
  const formatBirthday = (dateString) => {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { day: 'numeric', month: 'long', year: 'numeric' });
  };

  const calculateAge = (birthDate, deathDate) => {
    if (!birthDate || !deathDate) return '';
    const birth = new Date(birthDate);
    const death = new Date(deathDate);
    const ageInYears = Math.floor((death - birth) / (365.25 * 24 * 60 * 60 * 1000));
    return ` (died aged ${ageInYears})`;
  };

  const formatDateWithIcons = (birthDate, deathDate) => {
    const formattedBirth = birthDate ? `* ${formatBirthday(birthDate)}` : '';
    const formattedDeath = deathDate
      ? `<br />† ${formatBirthday(deathDate)}${calculateAge(birthDate, deathDate)}`
      : '';
    return `${formattedBirth}${formattedDeath}`;
  };

  function generateCrewCards(crewList, peopleMap) {
    if (!Array.isArray(crewList) || crewList.length === 0) return "N/A";

    // Track first occurrence per department (normalized key).
    const firstRendered = {};

    return crewList.map(credit => {
      const personData = peopleMap.get(Number(credit.personID)) || {};
      const profileUrl = credit.profile_path
        ? `<img src="${tmdbImgBaseUrl}w185${credit.profile_path}" loading="lazy" alt="${credit.name}" class="profile-image" />`
        : `${svgPlaceholderProfile}`;

      const imdbLink = personData.imdb_id
        ? `https://www.imdb.com/name/${personData.imdb_id}`
        : `${tmdbBaseUrl}person/${credit.personID}`;

      // Combine the roles.
      const combinedRoles = credit.jobs.sort((a, b) => a.localeCompare(b)).join(' / ');
      // const allRoles = credit.originalJobs.join('\n- '); // ### unused

      // Get episode count for TV shows
      const episodeCount = (isSeries && credit && credit.episode_count) ? 
      (() => {
        const count = Array.isArray(credit.episode_count) 
          ? credit.episode_count.reduce((sum, count) => sum + count, 0)
          : Number(credit.episode_count);
        return `${count} ${count === 1 ? 'Episode' : 'Episodes'}`;
      })()
      : '';

      // Build the custom tooltip
      const tooltipBirth = formatDateWithIcons(personData.birthday, personData.deathday);
      
      const tooltipPlace = personData.place_of_birth
        ? `Birthplace: ${personData.place_of_birth}`
        : '';
      const tooltipBio = personData.biography
        ? (() => {
          const textarea = document.createElement('textarea');
          textarea.innerHTML = personData.biography;
          return textarea.value
            .replace(/\u200B|\u200C|\u200D|\u200E|\u200F|\uFEFF/gu, '') // Remove various zero-width characters and BOM
            .replace(/\u00A0/g, ' ') // Replace actual non-breaking space character
            .replace(/(\S+) \((\d{4})\)/g, '$1\u00A0($2)'); // Then add back only where needed
        })()
        : '';

      // Check if there's any meaningful information to display
      const hasContent = Boolean(tooltipBio ||
        (personData.birthday || personData.deathday) ||
        personData.place_of_birth);

      // Create appropriate tooltip content
      const tooltipContent = hasContent
        ? `<div class="tooltip-content">
        <div class="tooltip-bio">${tooltipBio}</div>
        <div class="tooltip-bio-data">${tooltipBirth}</div>
        <div class="tooltip-bio-data">${tooltipPlace}</div>
        </div>`
        : `<div class="tooltip-content empty">
              <div class="no-info">No information available</div>
            </div>`;

      const crewTooltip = `
      <div class="card-ttip" data-tt-pos="right">
          ${tooltipContent}
      </div>
    `;

      // If this is the first occurrence of this department, add an id
      let idAttribute = "";
      if (credit.department) {
        const normDep = credit.department.toLowerCase().replace(/\s+/g, '-');
        if (!firstRendered[normDep]) {
          firstRendered[normDep] = true;
          idAttribute = ` id="${normDep}"`;
        }
      }
      return `
      <div class="card person"${idAttribute} data-person-id="${credit.personID}" data-imdb-link="${imdbLink}"
           > 
        ${profileUrl}
        <div class="card-title">${credit.name}</div>
        <div class="card-subtitle">${episodeCount}</div>
        <div class="card-subtitle">${combinedRoles}</div>
        ${crewTooltip}
      </div>
    `;
    }).join("");
  }

  function generateCastCards(castList, peopleMap) {
    if (!Array.isArray(castList) || castList.length === 0) return "N/A";
    return castList.map((credit) => {
      const personData = peopleMap.get(Number(credit.personID)) || {};
      const profileUrl = credit.profile_path
        ? `<img src="${tmdbImgBaseUrl}w185${credit.profile_path}" loading="lazy" alt="${credit.name}" class="profile-image" />`
        : `${svgPlaceholderProfile}`;
      const imdbLink = personData.imdb_id
        ? `https://www.imdb.com/name/${personData.imdb_id}`
        : `${tmdbBaseUrl}person/${credit.personID}`;

      const character = credit.character || "";

      // Get episode count for TV shows
      const episodeCount = (isSeries && credit && credit.episode_count) ? 
      `${credit.episode_count} ${(Number(credit.episode_count) === 1) ? 'Episode' : 'Episodes'}` : '';

      const tooltipBirth = formatDateWithIcons(personData.birthday, personData.deathday);
      const tooltipPlace = personData.place_of_birth
        ? `Birthplace: ${personData.place_of_birth}`
        : '';
      const tooltipBio = personData.biography
        ? (() => {
          const textarea = document.createElement('textarea');
          textarea.innerHTML = personData.biography;
          return textarea.value
            .replace(/\u00A0/g, ' ') // Replace actual non-breaking space character
            .replace(/(\S+) \((\d{4})\)/g, '$1\u00A0($2)'); // Then add back only where needed
        })()
        : '';

      // Check if there's any meaningful information to display
      const hasContent = Boolean(tooltipBio ||
        (personData.birthday || personData.deathday) ||
        personData.place_of_birth);

      // Create appropriate tooltip content
      const tooltipContent = hasContent
        ? `<div class="tooltip-content">
          <div class="tooltip-bio">${tooltipBio}</div>
          ${tooltipBirth ? `<div class="tooltip-bio-data">${tooltipBirth}</div>` : ''}
          ${tooltipPlace ? `<div class="tooltip-bio-data">${tooltipPlace}</div>` : ''}
        </div>`
        : `<div class="tooltip-content empty">
          <div class="no-info">No information available</div>
        </div>`;

      const castTooltip = `
      <div class="card-ttip" data-tt-pos="right">
          ${tooltipContent}
      </div>
    `;
      return `
      <div class="card person" data-person-id="${credit.personID}" data-imdb-link="${imdbLink}">

        ${profileUrl}
        <div class="card-title">${credit.name}</div>
        <!--div class="card-subtitle">${episodeCount}</div-->
        ${character ? `<div class="card-subtitle">${character}</div>` : ""}
        ${castTooltip}
      </div>
    `;
    }).join("");
  }

  function generateMediaCards(mediaList, sort = true, parentMediaType = 'movie') {
    const sortedRecords = sort ? sortRecordsByRating(mediaList) : mediaList;
    
    return sortedRecords.map(record => {
      // Determine media type (use record's type if available, otherwise use parent's type)
      const mediaType = record.media_type || parentMediaType;
      
      // Get appropriate fields based on media type
      const title = mediaType === 'tv' 
        ? (record.name || record.original_name || "Untitled") 
        : (record.title || record.original_title || "Untitled");
      
      const dateField = mediaType === 'tv' ? record.first_air_date : record.release_date;
      const year = dateField ? dateField.substring(0, 4) : "";
      
      const posterUrl = record.poster_path
        ? `<img src="${tmdbImgBaseUrl}w185${record.poster_path}" loading="lazy" alt="${title}" class="media-image"/>`
        : createPlaceholderSVG(185, 278, 'poster');
      
      const rating = record.vote_average.toFixed(1);
      
      return `
        <div class="card media" title="Click to open on TMDB"
            data-tmdb-url="${tmdbBaseUrl}${mediaType}/${record.id}">
         ${posterUrl}
          <div class="card-title">${title}</div>
          <div class="card-year">${year}</div>
          <div class="card-rating">⭐ ${rating}</div>
        </div>
      `;
    }).join("");
  }

  function sortRecordsByRating(record) {
    if (!Array.isArray(record) || record.length === 0) return [];
    return record.sort((a, b) => b.vote_average - a.vote_average); // Descending order by vote_average
  }

  function generateImageCards(imageList, type, sort = true) {
    const sortedImages = sort
      ? imageList.sort((a, b) => (a.iso_639_1 || "").localeCompare(b.iso_639_1 || ""))
      : imageList;

    return sortedImages.map(image => {
      if (type === 'Backdrop') {
        const thumbUrl = `${tmdbImgBaseUrl}w500${image.file_path}`;
        return `
          <div 
            class="card backdrop"
            title="Click to Enlarge"
            data-file-path="${image.file_path}"
            data-imdbid="${mediaData.imdbID}"
            data-media-type="backdrop"
          >
            <img src="${thumbUrl}" alt="${type}" loading="lazy" />
          </div>
        `;
      } else { // Poster
        const imageUrl = `${tmdbImgBaseUrl}w500${image.file_path}`;
        return `
          <div 
            class="card poster"
            title="Click to set as Default"
            data-file-path="${image.file_path}"
            data-imdbid="${mediaData.imdbID}"
            data-media-type="poster"
          >
            <img src="${imageUrl}" alt="${type}" loading="lazy" />
          </div>
        `;
      }
    }).join("");
  }

  function generateVideoCards(videoArray) {
    const sortedVideos = sortVideos(videoArray);
    return sortedVideos.map(video => {
      const thumbnailUrl = video.site === "YouTube"
        ? `https://img.youtube.com/vi/${video.key}/hqdefault.jpg`
        : "";
      const fallbackUrl = video.site === "YouTube"
        ? `https://img.youtube.com/vi/${video.key}/default.jpg`
        : "";
      return `
        <div class="card video"
             title="Click to play video"
             data-video-key="${video.key}"
             data-media-type="video"
             data-video-site="${video.site}">
          <div class="thumbnail-container">
            ${thumbnailUrl ? `
              <img src="${thumbnailUrl}" alt="${video.name}" loading="lazy" class="video-thumbnail"
                   onerror="this.src='${fallbackUrl}'; this.onerror=null;">
              <div class="play-container">
                <svg class="yt play-icon">
                  <use href="#play_icon"></use>
                </svg>
              </div>
            ` : ""}
          </div>
          <div class="card-title">${video.name}</div>
        </div>
      `;
    }).join("");
  }

  function sortVideos(videos) {
    if (!Array.isArray(videos) || videos.length === 0) return [];
    return videos.sort((a, b) => {
      const priority = { Trailer: 1, Teaser: 2 };
      const typeA = priority[a.type] || 3; // Default priority for other types
      const typeB = priority[b.type] || 3;
      if (typeA !== typeB) return typeA - typeB; // Sort by priority
      return a.name.localeCompare(b.name); // Sort alphabetically within same type
    });
  }

  // Generate season cards
  function generateSeasonCards(seasons, showId) {
    if (!Array.isArray(seasons) || seasons.length === 0) return "N/A";

    // Filter out season 0 with 0 episodes
    const filteredSeasons = seasons.filter(season =>
      !(season.season_number === 0 && season.episode_count === 0)
    );

    if (filteredSeasons.length === 0) return "N/A";

    return filteredSeasons.map(season => {
      const posterUrl = season.poster_path
        ? `${tmdbImgBaseUrl}w500${season.poster_path}`
        : createPlaceholderSVG(185, 278, 'poster');

      /*
      const airDate = (season.season_number === 0 && !season.air_date)
        ? ""
        : (season.air_date
          ? new Date(season.air_date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
          : 'TBA');
     */
      const tooltipContent = season.overview ? `<div class="tooltip-content"><div class="tooltip-bio">${season.overview}</div></div>` : `<div class="tooltip-content empty">No information available</div>`;

      const seasonTooltip = `
      <div class="card-ttip" data-tt-pos="right">
          ${tooltipContent}
      </div>
    `;   
    
      return `
  <div class="card season" data-type="season" data-season="${season.season_number}" data-episodes="${season.episode_count}" data-show-id="${showId}" data-imdb-id="${mediaData.imdbID}">
      ${season.poster_path
        ? `<img src="${posterUrl}" loading="lazy" alt="${season.name}" class="media-image" />`
          : posterUrl
        }
      <div class="card-title">${season.name}</div>
      <div class="card-subtitle">${season.episode_count} Episode${season.episode_count !== 1 ? 's' : ''}</div>
      <div class="card-year">${season.season_number === 0 && !season.air_date ? '' : (season.air_date ? season.air_date.split('-')[0] : 'TBA')}</div>
      <div class="card-rating">${season.vote_average ? '⭐ ' + season.vote_average.toFixed(1) : ''}</div>
      ${seasonTooltip}
    </div>
  `;
    }).join("");
  }

  const seasonCardsHTML = isSeries && seasons.length > 0
    ? `<div class="details-row">
      <div class="card-container media">
        ${generateSeasonCards(seasons, mediaData.tmdbID)}
      </div>
    </div>
    <div id="episodes-container" class="episodes-row"></div>`
    : "";

  const postersHTML = (mediaData.images && mediaData.images.posters && mediaData.images.posters.length)
    ? `<div class="details-row">
       <div class="header">Posters (${mediaData.images.posters.length})</div>
       <div class="card-container image">
         ${generateImageCards(mediaData.images.posters, 'Poster', true)}
       </div>
     </div>`
    : "";

  const backdropsHTML = (mediaData.images && mediaData.images.backdrops && mediaData.images.backdrops.length)
    ? `<div class="details-row">
       <div class="header">Backdrops (${mediaData.images.backdrops.length})</div>
       <div class="card-container image">
         ${generateImageCards(mediaData.images.backdrops, 'Backdrop', true)}
       </div>
     </div>`
    : "";

// Generate recommendations and similar as media pills.
const recommendationsHTML = (mediaData.recommendations &&
  Array.isArray(mediaData.recommendations.results) &&
  mediaData.recommendations.results.length)
  ? `<div class="details-row">
       <div class="header recommendations">Recommendations (${mediaData.recommendations.results.length})</div>
       <div class="card-container media">
         ${generateMediaCards(mediaData.recommendations.results, true, mediaData.mediaType)}
       </div>
     </div>`
  : "";

const similarHTML = (mediaData.similar &&
  Array.isArray(mediaData.similar.results) &&
  mediaData.similar.results.length)
  ? `<div class="details-row">
       <div class="header">Similar (${mediaData.similar.results.length})</div>
       <div class="card-container media">
         ${generateMediaCards(mediaData.similar.results, true, mediaData.mediaType)}
       </div>
     </div>`
  : "";

  // Generate video section.
  const videosHTML = (mediaData.videos &&
    Array.isArray(mediaData.videos.results) &&
    mediaData.videos.results.length)
    ? `<div class="details-row">
         <div class="header">Videos (${mediaData.videos.results.length})</div>
         <div class="card-container video">
           ${generateVideoCards(mediaData.videos.results)}
         </div>
       </div>`
    : "";

  // Build keywordsHTML from the keywords array (sorted alphabetically and clickable)
  const keywordsHTML = (keywords && keywords.length)
    ? `<div class="details-row keywords-container">
       ${keywords
      .sort((a, b) => a.name.localeCompare(b.name))
      .map(keyword => `
           <a href="${tmdbBaseUrl}keyword/${keyword.id}" 
              class="keyword-pill" 
              target="_blank" 
              title="${keyword.name}">
             ${keyword.name}
           </a>
         `).join("")}
     </div>`
    : "";

  // --- Process Credits and Generate HTML ---
  const { orderedCrewCredits, castCredits } = processCredits(credits, mediaData, peopleMap, mediaType);

  // For TV series, prioritize "created_by" crew members to appear first
  let finalCrewCredits = [...orderedCrewCredits]; // Create a copy to avoid modifying the original

  if (isSeries &&
    mediaData.created_by &&
    Array.isArray(mediaData.created_by) &&
    mediaData.created_by.length > 0) {

    // Get creator IDs from mediaData directly
    // const creatorIds = mediaData.created_by.map(creator => creator.id);

    // Create "Created by" department entries for each creator
    const creatorCredits = mediaData.created_by.map(creator => {
      return {
        jobs: ["Creator"],
        originalJobs: ["Creator"],
        department: "Creator",  // Special department that will sort first
        name: creator.name,
        personID: creator.id,
        profile_path: creator.profile_path || null
      };
    });

    // Add these to the beginning of finalCrewCredits
    finalCrewCredits = [...creatorCredits, ...finalCrewCredits];
  }

  function generateCrewQuicklinks(crewCredits) {
    // Fixed department order – adjust as needed.
    const departmentOrder = [ "Creator",
      "Directing", "Writing", "Production", "Camera", "Editing", "Sound",
      "Art", "Costume & Make-Up", "Lighting", "Visual Effects",
      "Special Effects", "Stunts", "Music", "Post-Production", "Crew"
    ];

    // Compute counts for each department.
    const departmentCounts = {};
    crewCredits.forEach(credit => {
      if (credit.department) {
        departmentCounts[credit.department] = (departmentCounts[credit.department] || 0) + 1;
      }
    });

    // Total number of crew credits.
    const totalCrew = crewCredits.length;

    // Determine which departments are present.
    const presentDepartments = new Set();
    crewCredits.forEach(credit => {
      if (credit.department) {
        presentDepartments.add(credit.department);
      }
    });

    // Generate quicklinks only for departments that appear.
    // Normalize the department text to an id.
    const links = departmentOrder.filter(dep => presentDepartments.has(dep))
      .map(dep => {
        const normId = dep.toLowerCase().replace(/\s+/g, "-");
        // Create tooltip text using the department count and total crew.
        const count = departmentCounts[dep] || 0;
        const tooltipText = `<span class="ttip-txt" data-tt-pos="bottom" style="font-size: 1em;">${dep}<br/>(${count}/${totalCrew})</span>`;
        return `<a href="javascript:void(0)" class="dept-quicklink" data-target-id="${normId}" data-tooltip>${dep}${tooltipText}</a>`;
      });
    return `<div class="crew-quicklinks">${links.join(" ")}</div>`;
  }

  // Generate team quicklinks based on the ordered credits.
  // const crewQuicklinksHTML = generateCrewQuicklinks(orderedCrewCredits);
  const crewQuicklinksHTML = generateCrewQuicklinks(finalCrewCredits);

  const crewHTML = `<div class="details-row">
  ${crewQuicklinksHTML}
  <div class="card-container person">${generateCrewCards(finalCrewCredits, peopleMap)}
  </div>
</div>`;

  const castHTML = `<div class="details-row">
         <!--h3>Cast (${castCredits.length})</h3-->
         <div class="card-container person">${generateCastCards(castCredits, peopleMap)}
         </div>
       </div>`;

  // --- Assemble the Detail View HTML Template ---
  const detailViewHTML = `
    <button class="close-modal" data-popup-type="detail">×</button>
    ${customListIcon}${topRowIcons.outerHTML}
    <div class="detail-view" data-imdbid="${mediaData.imdbID}" data-tmdbid="${mediaData.tmdbID}" data-media-type="${mediaData.mediaType}" data-media-title="${mediaData.Title}">
      <div class="poster-column">
        <div class="poster-wrapper">
          <img id="detail-poster"
               src="${posterSrc}" loading="lazy"
               alt="${mediaData.Title} Poster"
               class="poster-image"
               data-defaultsrc="${defaultPoster}" />
          ${showToggleButton ? `<button id="toggle-active-poster" class="toggle-active-poster">Revert to Default</button>` : ""}
        </div>
      </div>
      <div class="details-column">
        <div class="details-header">
          <div class="detail-title">
            <span class="title">${mediaData.Title}</span>
            <span class="year">${mediaData.Year}</span>
          </div>
          ${originalTitleHTML}
          <div class="details-row">
            <div class="ratings-container">${ratingsHTML}${awardsIcon}
            </div>
            <div>
              <span class="details-content"><span class="rated">${mediaData.Rated || 'N/A'}</span>${mediaData.Runtime && mediaData.Runtime !== "N/A" ? '&nbsp;&nbsp;' + mediaData.Runtime + '<span class="bullet"></span>' : '&nbsp;'}
${country}<span class="bullet"></span>${languages}${tvStatusHTML}
              </span>
              </div>
          </div>
          <div class="details-row genres">
            <div class="genres">${genres}</div>
            ${financialsHTML}
          </div>
        </div>
        <div id="details-content-container" class="details-content">
          <div class="details-row">
            ${taglineHTML}
            ${plotHTML}
          </div>
          ${seasonCardsHTML}
            ${crewHTML}
            ${castHTML}
            ${recommendationsHTML}
            ${similarHTML}
            ${videosHTML}    
            ${collectionHTML}
            ${postersHTML}
            ${backdropsHTML}
            ${keywordsHTML}
          <div class="details-row">
            ${companiesAndNetworksHTML}
          </div>
        </div>
      </div>
    </div>
  `;
  return detailViewHTML;
}

// === CREDIT PROCESSING ===
function processCredits(credits, movieData, peopleMap, mediaType) {
  const grouped = groupCrewCredits(credits);
  // Separate only directors and writers; everything else goes into "others"

  const categorized = categorizeGroupedCredits(grouped.crew);

  const finalDirectors = handleDirectorFallbacks(categorized.directors, movieData);
  const finalWriters = handleWriterFallbacks(categorized.writers, movieData);

  // Merge all other crew credits into one array.
  const sortedOthers = sortCrewSection(categorized.others, peopleMap);

  return {
    // Directors come first (regardless of department priority),
    // then writers, and then every other credit is sorted based on dept priority.
    orderedCrewCredits: [
      ...sortCrewSection(finalDirectors, peopleMap),
      ...sortCrewSection(finalWriters, peopleMap),
      ...sortedOthers
    ],
    castCredits: sortCastCredits(grouped.cast, peopleMap, mediaType)
  };
}

// === GROUPING LOGIC ===
function groupCrewCredits(credits) {
  const groups = new Map();
  const cast = [];

  // Removed the unused departmentPriority mapping here.
  const jobPriority = {
    "Director": 1,
    "Screenplay": 2,
    "Story": 3,
    "Writer": 4,
    "Director of Photography": 1,
    "Editor": 1,
    "Original Music Composer": 1,
    "Production Designer": 1, // Head of the Art Department
    "Art Director": 2
  };

  credits.forEach(credit => {
    if (credit.role !== "crew") {
      cast.push(credit);
      return;
    }

    const key = `${credit.personID}-${credit.department}`;
    const existing = groups.get(key);

    if (existing) {
      existing.originalJobs.push(credit.job);

      // Store episode count if it exists
      if (credit.episode_count) {
        existing.episode_count = existing.episode_count || [];
        existing.episode_count.push(credit.episode_count);
      }

      existing.jobs = Array.from(new Set([...existing.jobs, credit.job]))
        .sort((a, b) => (jobPriority[a] || 100) - (jobPriority[b] || 100));
    } else {
      const episodeCount = credit.episode_count ? [credit.episode_count] : undefined;
      groups.set(key, {
        ...credit,
        jobs: [credit.job],
        originalJobs: [credit.job],
        episode_count: episodeCount,
        department: credit.department,
        personID: credit.personID,
        name: credit.name,
        profile_path: credit.profile_path
      });
    }
  });

  return { crew: Array.from(groups.values()), cast };
}

// === CATEGORIZATION ===
// Now we separate only directors and writers; everything else goes into a single 'others' array.
function categorizeGroupedCredits(crew) {
  const directors = [];
  const writers = [];
  const others = [];

  crew.forEach(credit => {
    if (credit.jobs && Array.isArray(credit.jobs) && credit.jobs.includes("Director")) {
      directors.push(credit);
    } else if (
      credit.department === "Writing" ||
      (credit.jobs && Array.isArray(credit.jobs) && credit.jobs.some(j => j && typeof j === 'string' && j.toLowerCase().includes("writer")))
    ) {
      writers.push(credit);
    } else {
      others.push(credit);
    }
  });

  return { directors, writers, others };
}

/// === FALLBACK HANDLERS (INTEGRATED WITH GROUPING) ===
function handleDirectorFallbacks(existingDirectors, movieData) {
  if (existingDirectors.length > 0 || !movieData.Director) return existingDirectors;

  const cachedDirectors = Array.isArray(movieData.Director)
    ? movieData.Director
    : [movieData.Director];

  return cachedDirectors.map(name => ({
    jobs: ["Director"],
    originalJobs: ["Director"],
    department: "Directing",
    name: name,
    personID: null,
    profile_path: null
  }));
}

function handleWriterFallbacks(existingWriters, movieData) {
  if (existingWriters.length > 0 || !movieData.Writer) return existingWriters;

  const cachedWriters = Array.isArray(movieData.Writer)
    ? movieData.Writer
    : [movieData.Writer];

  return cachedWriters.map(name => ({
    jobs: ["Writer"],
    originalJobs: ["Writer"],
    department: "Writing",
    name: name,
    personID: null,
    profile_path: null
  }));
}

// === SORTING ===
function sortCrewSection(credits, peopleMap) {
  // Updated mapping for department ordering according to industry standards:
  // Directing:1, Writing:2, Production:3, Camera:4, Editing:5, Sound:6, Art:7, etc.
  const departmentPriority = {
    "Creator": 0,
    "Directing": 1,
    "Writing": 2,
    "Production": 3,
    "Camera": 4,
    "Editing": 5,
    "Sound": 6,
    "Art": 7,
    "Costume & Make-Up": 8,
    "Lighting": 9,
    "Visual Effects": 10,
    "Special Effects": 11,
    "Stunts": 12,
    "Music": 13,
    "Post-Production": 14,
    "Crew": 15 // Generic Crew comes last
  };

  const jobPriority = {
    "Director": 1,
    "Screenplay": 2,
    "Story": 3,
    "Writer": 4,
    "Director of Photography": 1,
    "Editor": 1,
    "Original Music Composer": 1,
    "Production Designer": 1,
    "Art Director": 2,
  };

  return credits.sort((a, b) => {
    // 1. Department priority
    const deptA = departmentPriority[a.department] || 100;
    const deptB = departmentPriority[b.department] || 100;
    if (deptA !== deptB) return deptA - deptB;

    // 2. Highest priority job in group
    const jobA = Math.min(...a.jobs.map(j => jobPriority[j] || 100));
    const jobB = Math.min(...b.jobs.map(j => jobPriority[j] || 100));
    if (jobA !== jobB) return jobA - jobB;

    // 3. Popularity for directors/writers
    if (['Directing', 'Writing'].includes(a.department)) {
      const personA = peopleMap.get(a.personID);
      const personB = peopleMap.get(b.personID);
      const popA = personA?.popularity || 0;
      const popB = personB?.popularity || 0;
      if (popA !== popB) return popB - popA;
    }

    // 4. Alphabetical fallback
    return a.name.localeCompare(b.name);
  });
}

function sortCastCredits(castCredits, peopleMap, mediaType) {
  // For movies, use the original sorting logic
  if (mediaType !== 'tv') {
    return castCredits.sort((a, b) => {
      // Primary sort: order field (including 0)
      if (a.order !== undefined && b.order !== undefined) {
        return a.order - b.order;
      } else if (a.order !== undefined) {
        return -1; // a has order, but b doesn't
      } else if (b.order !== undefined) {
        return 1;  // b has order, but a doesn't
      }

      // Secondary sort: popularity from people store
      const personA = peopleMap.get(a.personID);
      const personB = peopleMap.get(b.personID);
      const popularityA = personA?.popularity !== undefined ? personA.popularity : 0;
      const popularityB = personB?.popularity !== undefined ? personB.popularity : 0;

      if (popularityA !== popularityB) {
        return popularityB - popularityA; // Higher popularity first
      }

      // Tertiary sort: alphabetical by name
      return a.name.localeCompare(b.name);
    });
  }
  
  // For TV shows, combine roles for the same actor
  const groupedByActor = new Map();
  
  // Group by actor ID
  castCredits.forEach(credit => {
    if (!groupedByActor.has(credit.personID)) {
      // Create a new entry with all original properties
      groupedByActor.set(credit.personID, {
        ...credit, 
        characterDetails: [{
          character: credit.character,
          episodeCount: credit.episode_count
        }]
      });
    } else {
      // Add this character to the existing entry
      const existing = groupedByActor.get(credit.personID);
      existing.characterDetails.push({
        character: credit.character,
        episodeCount: credit.episode_count
      });
      
      // Keep the lowest order value
      if (credit.order !== undefined && (existing.order === undefined || credit.order < existing.order)) {
        existing.order = credit.order;
      }
      
      // Keep the highest episode count for the main credit
      if (credit.episode_count && (!existing.episode_count || credit.episode_count > existing.episode_count)) {
        existing.episode_count = credit.episode_count;
      }
    }
  });
  
  // Convert back to array and add combined character field
  const combinedCredits = Array.from(groupedByActor.values()).map(credit => {
    // Combine all characters into one string with episode counts
    credit.character = credit.characterDetails
      .filter(detail => detail.character)
      .map(detail => {
        const character = detail.character;
        const episodeCount = detail.episodeCount;
        return episodeCount ? `${character} (${episodeCount})` : character;
      })
      .join(' / ');
    
    delete credit.characterDetails; // Remove the temporary array
    return credit;
  });
  
  // Sort using the same logic as before
  return combinedCredits.sort((a, b) => {
    // Primary sort: order field (including 0)
    if (a.order !== undefined && b.order !== undefined) {
      return a.order - b.order;
    } else if (a.order !== undefined) {
      return -1; // a has order, but b doesn't
    } else if (b.order !== undefined) {
      return 1;  // b has order, but a doesn't
    }

    // Secondary sort: popularity from people store
    const personA = peopleMap.get(a.personID);
    const personB = peopleMap.get(b.personID);
    const popularityA = personA?.popularity !== undefined ? personA.popularity : 0;
    const popularityB = personB?.popularity !== undefined ? personB.popularity : 0;

    if (popularityA !== popularityB) {
      return popularityB - popularityA; // Higher popularity first
    }

    // Tertiary sort: alphabetical by name
    return a.name.localeCompare(b.name);
  });
}

function buildTMDbImageUrl(filePath, type, desiredSize) {
  // Check if filePath is null, undefined, or "null" string
  if (!filePath || filePath === "null") {
    // Return placeholder SVG based on the content type
    return createPlaceholderSVG(
      type === 'poster' ? 185 : 300,
      type === 'poster' ? 278 : 169,
      type
    );
  }

  // If filePath is already a full URL (including Amazon URLs), return as-is
  if (filePath.startsWith('http')) {
    // Check for malformed URLs that contain both TMDb and Amazon patterns
    if (filePath.includes('tmdb.org/t/p') && filePath.includes('m.media-amazon.com')) {
      const amazonMatch = filePath.match(/(https:\/\/m\.media-amazon\.com\/.*)/);
      if (amazonMatch && amazonMatch[1]) {
        return amazonMatch[1]; // Return just the Amazon URL part
      }
    }
    return filePath;
  }

  // Otherwise build TMDb URL
  const defaultSizes = {
    backdrop: "w1280",
    poster: "w500",
    logo: "w185",
    still: "w300"
  };
  const size = desiredSize || defaultSizes[type] || "w500";
  return `https://image.tmdb.org/t/p/${size}${filePath}`;
}

function handleMouseActivity() {
  stopSlideshow();
  resetTimer();
}

// --- KEYSTROKE LISTENER CODE ---
function handleModalKeydown(event) {
  const modal = document.getElementById("imageModal");
  if (modal && !modal.classList.contains("hidden")) {
    switch (event.key) {
      case "ArrowLeft":
        stopSlideshow();
        {
          const prevButton = document.getElementById("prev-backdrop");
          if (prevButton && !prevButton.classList.contains("hidden")) {
            prevButton.click();
          }
        }
        break;
      case "ArrowRight":
        stopSlideshow();
        {
          const nextButton = document.getElementById("next-backdrop");
          if (nextButton && !nextButton.classList.contains("hidden")) {
            nextButton.click();
          }
        }
        break;
      case "Escape":
        stopSlideshow();
        closePopup('image');
        break;
      case " ":
        event.preventDefault();
        startSlideshow();
        break;
      default:
        stopSlideshow();
        break;
    }
  }
}

function addModalKeyListeners() {
  document.addEventListener("keydown", handleModalKeydown);
}

function removeModalKeyListeners() {
  document.removeEventListener("keydown", handleModalKeydown);
}

// --- MOUSE INACTIVITY / RESET TIMER CODE ---
function resetTimer() {
  clearTimeout(mouseTimer); // Clear any existing timer
  const modal = document.getElementById("imageModal");
  if (!modal) return;

  const controls = modal.querySelectorAll('.nav-backdrop, .toggle-active-backdrop, #backdrop-counter, .close-modal');
  controls.forEach(control => {
    control.classList.remove("fade");
  });

  // Set a new timer
  mouseTimer = setTimeout(() => {
    controls.forEach(control => {
      control.classList.add("fade");
    });
  }, inactivityTime);
}

/**
 * Opens a unified image modal for both posters and backdrops.
 * For "poster": updates the detail view poster image.
 * For "backdrop": displays the image in a static modal (id="imageModal").
 *
 * @param {string} filePath - The file path for the image (e.g. "/emYeJVZrpl2WfndJlhbls1e7lzQ.jpg").
 * @param {string} imdbID - The IMDb ID of the movie.
 * @param {string} mediaType - Either "poster" or "backdrop".
 */
function openImageModal(filePath, imdbID, mediaType) {
  if (mediaType === "poster") {
    const posterImg = document.getElementById("detail-poster");
    if (!posterImg) return;
    const defaultFilePath = posterImg.getAttribute("data-defaultsrc") || "";

    // Build full URL for display using w780 (default).
    posterImg.src = buildTMDbImageUrl(filePath, "poster");
    if (filePath !== defaultFilePath) {
      // Store only the file_path.
      setMediaCustomization(imdbID, "poster", filePath);
      const media = mediaCache.get(imdbID);
      if (media) {
        media.Poster = filePath;
        mediaCache.set(imdbID, media);
      }
      // Create / update toggle button...
      let toggleBtn = document.getElementById("toggle-active-poster");
      if (!toggleBtn) {
        const posterWrapper = document.querySelector(".poster-wrapper");
        if (posterWrapper) {
          toggleBtn = document.createElement("button");
          toggleBtn.id = "toggle-active-poster";
          toggleBtn.textContent = "Revert to Default";
          toggleBtn.classList.add("toggle-active-poster");
          posterWrapper.appendChild(toggleBtn);
        }
      } else {
        toggleBtn.textContent = "Revert to Default";
      }
      toggleBtn.onclick = function () {
        clearMediaCustomization(imdbID, "poster");
        const media = mediaCache.get(imdbID);
        if (media) {
          media.Poster = media.defaultPoster || defaultFilePath;
          mediaCache.set(imdbID, media);
        }
        posterImg.src = buildTMDbImageUrl(defaultFilePath, "poster");
        this.remove();
        const gridCard = document.querySelector(`.media-card[data-imdbid="${imdbID}"]`);
        if (gridCard) {
          const gridPoster = gridCard.querySelector('.media-container img');
          if (gridPoster) {
            gridPoster.src = buildTMDbImageUrl(defaultFilePath, "poster");
          }
        }
      };
      const gridCard = document.querySelector(`.media-card[data-imdbid="${imdbID}"]`);
      if (gridCard) {
        const gridPoster = gridCard.querySelector('.media-container img');
        if (gridPoster) {
          gridPoster.src = buildTMDbImageUrl(filePath, "poster");
        }
      }
    } else {
      clearMediaCustomization(imdbID, "poster");
      const media = mediaCache.get(imdbID);
      if (media) {
        media.Poster = media.defaultPoster || defaultFilePath;
        mediaCache.set(imdbID, media);
      }
      const toggleBtn = document.getElementById("toggle-active-poster");
      if (toggleBtn) {
        toggleBtn.remove();
      }
      const gridCard = document.querySelector(`.media-card[data-imdbid="${imdbID}"]`);
      if (gridCard) {
        const gridPoster = gridCard.querySelector('.media-container img');
        if (gridPoster) {
          gridPoster.src = buildTMDbImageUrl(defaultFilePath, "poster");
        }
      }
    }
  } else if (mediaType === "backdrop") {
    const modal = document.getElementById("imageModal");
    if (!modal) return;
          // Get overlay and popup elements
          const overlay = document.getElementById("popup-overlay");
          const popup = document.getElementById("popup");
          
          // Show overlay and popup if they're hidden
          overlay.classList.remove("hidden");
          popup.classList.remove("hidden");
    modal.dataset.imdbid = imdbID;
    const modalImage = document.getElementById("imageModalImage");
    // For display, build the full URL from the raw file path.
    modalImage.src = buildTMDbImageUrl(filePath, "backdrop");

    const media = mediaCache.get(imdbID);
    // Build defaultBackdrop from the stored default raw file path.
    const defaultFilePath = (media && media.defaultBackdrop) ? media.defaultBackdrop : "";

    // Populate navigation array from movie.images.backdrops:
    if (media && media.images && media.images.backdrops) {
      // Get an array of raw file paths (not full URLs).
      const rawPaths = media.images.backdrops.map(img => img.file_path);
      currentBackdrops = rawPaths; // Store raw file paths!
      currentBackdropIndex = rawPaths.indexOf(filePath);
      if (currentBackdropIndex === -1) currentBackdropIndex = 0;
      document.getElementById('backdrop-counter').textContent =
        `${currentBackdropIndex + 1}/${currentBackdrops.length}`;
      // Show/hide navigation buttons based on position
      const prevButton = document.getElementById('prev-backdrop');
      const nextButton = document.getElementById('next-backdrop');
      
      if (prevButton) {
        prevButton.classList.toggle('hidden', currentBackdropIndex <= 0);
      }
      
      if (nextButton) {
        nextButton.classList.toggle('hidden', currentBackdropIndex >= currentBackdrops.length - 1);
      }
    }

    // Remove any previous mouse event listeners (to avoid duplicates)
    modal.removeEventListener('mousemove', handleMouseActivity);
    modal.removeEventListener('mouseenter', resetTimer);
    modal.addEventListener('mousemove', handleMouseActivity);
    modal.addEventListener('mouseenter', resetTimer);
    modal.addEventListener('click', handleMouseActivity);
    document.addEventListener('keydown', function () {
      // Only reset timer if the modal is visible
      if (!modal.classList.contains('hidden')) {
        handleMouseActivity();
      }
    });
    addModalKeyListeners();

    updateBackdropToggleState(imdbID, filePath, defaultFilePath);
    modal.classList.remove("hidden");
    modal.classList.add("active");
    resetTimer();
  }
}

// slideshow dissolve
function crossfadeToSlide(index) {
  const modal = document.getElementById("imageModal");
  const baseImg = document.getElementById("imageModalImage");
  const newSrc = buildTMDbImageUrl(currentBackdrops[index], "backdrop");

  // Create a new overlay image element dynamically:
  const overlayImg = document.createElement("img");
  overlayImg.className = "dynamic-overlay";
  // Apply our defaults:
  overlayImg.classList.remove("hidden"); // Use class instead of style.display
  overlayImg.style.opacity = "0";
  overlayImg.src = newSrc;
  // Append overlay inside the modal:
  modal.appendChild(overlayImg);

  // Force reflow so that the transition registers:
  void overlayImg.offsetWidth;

  // Begin crossfade: fade overlay in. (We do not fade out the base,
  // so that the change is a smooth dissolve.)
  overlayImg.style.opacity = "1";

  // When the overlay finishes its opacity transition…
  overlayImg.addEventListener("transitionend", function onTransitionEnd(event) {
    if (event.propertyName === "opacity" && overlayImg.style.opacity === "1") {
      // Update base image with new source.
      baseImg.src = newSrc;
      baseImg.style.opacity = "1";
      // Remove overlay from DOM.
      modal.removeChild(overlayImg);
      overlayImg.removeEventListener("transitionend", onTransitionEnd);
    }
  });

  // Update controls depending on mode:
  if (slideshowInterval) {
    hideControls();
  } else {
    showControls(index);
  }
}

// Function to advance to the next image (wraps around to the first)
function nextSlide() {
  if (currentBackdropIndex < currentBackdrops.length - 1) {
    currentBackdropIndex++;
  } else {
    currentBackdropIndex = 0;
  }
  // showSlide(currentBackdropIndex);
  crossfadeToSlide(currentBackdropIndex);
}

function startSlideshow() {
  if (slideshowInterval) return;
  // Remove any lingering dynamic overlay (if any)
  const existingOverlay = document.querySelector(".dynamic-overlay");
  if (existingOverlay) {
    existingOverlay.parentNode.removeChild(existingOverlay);
  }

  slideshowInterval = setInterval(nextSlide, 3000);
  hideControls();  // Hide controls while in slideshow.
  const modal = document.getElementById("imageModal");
  if (modal) {
    modal.classList.add("hide-cursor");
  }
}

function stopSlideshow() {
  if (slideshowInterval) {
    clearInterval(slideshowInterval);
    slideshowInterval = null;
  }
  // Remove any dynamic overlay that might exist.
  const existingOverlay = document.querySelector(".dynamic-overlay");
  if (existingOverlay) {
    // Also assign its src to base if available.
    const baseImg = document.getElementById("imageModalImage");
    if (existingOverlay.src) { baseImg.src = existingOverlay.src; }
    existingOverlay.parentNode.removeChild(existingOverlay);
  }
  // Ensure controls reappear:
  showControls(currentBackdropIndex);
  const modal = document.getElementById("imageModal");
  if (modal) {
    modal.classList.remove("hide-cursor");
  }
}

// sildeshow helper
function hideControls() {
  document.getElementById("backdrop-counter").classList.add("hidden");
  document.getElementById("prev-backdrop").classList.add("hidden");
  document.getElementById("next-backdrop").classList.add("hidden");
  document.getElementById("toggle-active-backdrop").classList.add("hidden");
  const closeBtn = document.querySelector(".close-modal[data-popup-type='image']");
  if (closeBtn) { closeBtn.classList.add("hidden"); }
}

// sildeshow helper
function showControls(index) {
  const counterElement = document.getElementById("backdrop-counter");
  const prevButton = document.getElementById("prev-backdrop");
  const nextButton = document.getElementById("next-backdrop");
  const toggleButton = document.getElementById("toggle-active-backdrop");
  //const closeBtn = document.querySelector(".close-modal");
  
  // Show counter and update text
  counterElement.classList.remove("hidden");
  counterElement.textContent = `${index + 1}/${currentBackdrops.length}`;
  
  // Show/hide prev button based on index
  if (index > 0) {
    prevButton.classList.remove("hidden");
  } else {
    prevButton.classList.add("hidden");
  }
  
  // Show/hide next button based on index
  if (index < currentBackdrops.length - 1) {
    nextButton.classList.remove("hidden");
  } else {
    nextButton.classList.add("hidden");
  }
  
  // Show toggle button
  toggleButton.classList.remove("hidden");
  
  // Show close button if it exists
  const closeBtn = document.querySelector(".close-modal[data-popup-type='image']");
  if (closeBtn) { 
    closeBtn.classList.remove("hidden"); 
  }
}

// Combined function to load all media data
async function loadMediaData(tmdbID, mediaType = 'movie') {
  // Determine store prefix based on media type
  const storePrefix = mediaType === 'tv' ? 'series' : 'movie';
  
  async function fetchFromStore(dataType) {
    return new Promise((resolve, reject) => {
      const storeName = `${storePrefix}_${dataType}`;
      const transaction = db.transaction([storeName], "readonly");
      const store = transaction.objectStore(storeName);
      
      // Determine if we should use the index based on data type
      const useIndex = dataType !== 'details';
      let request;
      
      if (useIndex) {
        const index = store.index("tmdbID");
        request = index.getAll(tmdbID);
      } else {
        request = store.get(tmdbID);
      }
      
      request.onsuccess = async () => {
        try {
          // Handle special case for keywords
          if (dataType === 'keywords' && request.result && request.result.length > 0) {
            const keywordIDs = request.result.map(record => record.keywordID);
            const keywordInfo = await getKeywordNames(keywordIDs);
            resolve(keywordInfo);
          } else {
            resolve(request.result || (useIndex ? [] : null));
          }
        } catch (error) {
          reject(`Error processing ${storeName} data: ${error}`);
        }
      };
      
      request.onerror = () => {
        reject(`Failed to load ${storeName}`);
      };
    });
  }

  try {
    // Fetch details and credits (can be done in parallel)
    const [details, credits] = await Promise.all([
      fetchFromStore('details'),
      fetchFromStore('credits')
    ]);
    
    // Extract all distinct personIDs from the credits
    const personIDs = [...new Set(
      (credits || [])
        .filter(credit => credit.personID)
        .map(credit => credit.personID)
    )];
    
    // Fetch people and keywords (can be done in parallel)
    const [peopleMap, keywords] = await Promise.all([
      getPeopleInfo(personIDs),
      fetchFromStore('keywords')
    ]);
    
    return { details, credits, peopleMap, keywords };
  } catch (error) {
    console.error(`Error loading ${mediaType} data:`, error);
    throw error;
  }
}

// Given an array of personIDs, look up all person info from the "people" store.
function getPeopleInfo(personIDs) {
  return new Promise((resolve, reject) => {
    if (!Array.isArray(personIDs) || personIDs.length === 0) {
      resolve(new Map());
      return;
    }
    const transaction = db.transaction(["people"], "readonly");
    const store = transaction.objectStore("people");
    const peopleMap = new Map();
    let pending = personIDs.length;
    personIDs.forEach(pid => {
      const req = store.get(pid);
      req.onsuccess = () => {
        peopleMap.set(pid, req.result);
        pending--;
        if (pending === 0) resolve(peopleMap);
      };
      req.onerror = () => {
        pending--;
        if (pending === 0) resolve(peopleMap);
        reject(`Failed to load person info for ID: ${pid}`);
      };
    });
  });
}

// Given an array of keyword IDs, look up each keyword's full info from the "keywords" store.
// Here each record in the keywords store contains "keywordID" and "name".
function getKeywordNames(keywordIDs) {
  return new Promise((resolve, reject) => {
    if (!Array.isArray(keywordIDs) || keywordIDs.length === 0) {
      resolve([]);
      return;
    }
    const transaction = db.transaction(["keywords"], "readonly");
    const store = transaction.objectStore("keywords");
    const keywords = [];
    let pending = keywordIDs.length;
    keywordIDs.forEach(keywordID => {
      const req = store.get(keywordID);
      req.onsuccess = () => {
        if (req.result) {
          keywords.push({ id: req.result.keywordID, name: req.result.name });
        }
        pending--;
        if (pending === 0) resolve(keywords);
      };
      req.onerror = () => {
        pending--;
        if (pending === 0) resolve(keywords);
        reject(`Failed to load keyword for ID: ${keywordID}`);
      };
    });
  });
}

function collapseSeasonEpisodes(seasonNumber) {
  // Remove expanded class from season card
  const seasonCard = document.querySelector(`.card.season[data-season="${seasonNumber}"]`);
  seasonCard.classList.remove('expanded');
  seasonCard.classList.remove('focused');

  // Remove all episode cards for this season
  const episodeCards = document.querySelectorAll(`.card.episode[data-season="${seasonNumber}"]`);
  episodeCards.forEach(card => {
    // Add collapse animation class
    card.classList.add('collapsing');
    // Remove card after animation completes
    setTimeout(() => card.remove(), 300);
  });
}

async function loadSeasonEpisodes(showId, seasonNumber, imdbID) {
  const context = {
    db,
    showId,
    seasonNumber,
    imdbID,
  };

  try {
    // Check if this season is already expanded
    const seasonCard = document.querySelector(`.card.season[data-season="${seasonNumber}"]`);
    const isExpanded = seasonCard.classList.contains('expanded');

    // First, collapse any other expanded seasons
    document.querySelectorAll('.card.season.expanded').forEach(card => {
      if (parseInt(card.dataset.season) !== seasonNumber) {
        // Collapse other expanded seasons
        collapseSeasonEpisodes(parseInt(card.dataset.season));
      }
    });

    // Toggle expansion state for the clicked season
    if (isExpanded) {
      // If already expanded, collapse it and clear filters
      seasonCard.classList.remove('expanded');
      seasonCard.classList.remove('focused');
      resetCreditFilters();

      // Remove all episode cards
      const episodeCards = document.querySelectorAll(`.card.episode[data-season="${seasonNumber}"]`);
      episodeCards.forEach(card => {
        card.classList.add('collapsing');
        setTimeout(() => card.remove(), 300);
      });

      return;
    }

    // Season is not expanded, so expand it
    seasonCard.classList.add('focused');
    seasonCard.classList.add('expanded');

    // Check if episodes already exist in IndexedDB
    let episodeData = [];
    {
      const transaction = db.transaction("series_episodes", "readonly");
      const store = transaction.objectStore("series_episodes");
      const index = store.index("tmdbID");

      episodeData = await new Promise((resolve, reject) => {
        const request = index.getAll(showId);
        request.onsuccess = () => resolve(request.result.filter(ep => ep.season_number === seasonNumber));
        request.onerror = () => reject(request.error);
      });
    }

    // Check if we need to refresh data based on timestamp
    let needsRefresh = false;

    try {
      const seriesTransaction = db.transaction("series", "readonly");
      const seriesStore = seriesTransaction.objectStore("series");
      const seriesIndex = seriesStore.index("tmdbID");

      const seriesRecords = await new Promise((resolve) => {
        const request = seriesIndex.getAll(showId);
        request.onsuccess = () => resolve(request.result || []);
      });

      if (seriesRecords.length > 0) {
        const timestamp = seriesRecords[0].timestamp;

        if (timestamp) {
          const daysSinceUpdate = (Date.now() - timestamp) / (1000 * 60 * 60 * 24);

          // Only check for changes if more than 14 days have passed
          if (daysSinceUpdate > 14) {
            // Get season ID to check for changes
            let seasonId = null;
            const transaction = db.transaction("series_details", "readonly");
            const store = transaction.objectStore("series_details");

            const seriesData = await new Promise((resolve, reject) => {
              const request = store.get(showId);
              request.onsuccess = () => resolve(request.result);
              request.onerror = () => reject(request.error);
            });

            const seasons = seriesData?.seasons || [];
            const currentSeason = seasons.find(s => s.season_number === seasonNumber);

            if (currentSeason && currentSeason.id) {
              seasonId = currentSeason.id;

              // Format date as YYYY-MM-DD for TMDb API (max 14 days back)
              const startDate = new Date(Math.max(timestamp, Date.now() - 14 * 24 * 60 * 60 * 1000))
                .toISOString().split('T')[0];

              // Check the changes endpoint
              try {
                const changesUrl = `${tmdbApiBaseUrl}tv/season/${seasonId}/changes?api_key=${apiKeyManager.getTmdbKey()}&start_date=${startDate}`;
                const changesResponse = await fetch(changesUrl);

                if (changesResponse.ok) {
                  const changesData = await changesResponse.json();
                  // Only refresh if there are actual changes
                  needsRefresh = changesData.changes && changesData.changes.length > 0;

                  if (!needsRefresh) {
                    console.log(`No changes detected for season ${seasonNumber} since ${startDate}`);
                  } else {
                    console.log(`Changes detected for season ${seasonNumber}, refreshing data`);
                  }
                } else {
                  // If error checking changes, default to refresh
                  console.error("Error checking for changes:", changesResponse.status);
                  needsRefresh = true;
                }
              } catch (error) {
                console.error("Error checking for changes:", error);
                needsRefresh = true;
              }
            } else {
              // If we can't get the season ID, default to refresh
              needsRefresh = true;
            }
          }
        } else {
          // No timestamp, should refresh
          needsRefresh = true;
        }
      }
    } catch (error) {
      console.error("Error checking timestamp:", error);
      needsRefresh = true;
    }

    // If episodes exist but need refresh, or if no episodes exist
    if ((episodeData.length > 0 && needsRefresh) || episodeData.length === 0) {
      // showNotification(`Fetching episodes for Season ${seasonNumber}...`, true);

      // Get season details to know how many episodes to fetch
      let currentSeason;
      {
        const transaction = db.transaction("series_details", "readonly");
        const store = transaction.objectStore("series_details");

        const seriesData = await new Promise((resolve, reject) => {
          const request = store.get(showId);
          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });

        const seasons = seriesData?.seasons || [];
        currentSeason = seasons.find(s => s.season_number === seasonNumber);
      }

      if (currentSeason && currentSeason.episode_count > 0) {
        // Fetch and store episodes
        await fetchAndStoreSeasonEpisodes(context, currentSeason.episode_count);

        // Update timestamp
        try {
          const updateTransaction = db.transaction("series", "readwrite");
          const updateStore = updateTransaction.objectStore("series");
          const updateIndex = updateStore.index("tmdbID");

          const seriesRecords = await new Promise((resolve) => {
            const request = updateIndex.getAll(showId);
            request.onsuccess = () => resolve(request.result || []);
          });

          if (seriesRecords.length > 0) {
            seriesRecords.forEach(record => {
              record.timestamp = Date.now();
              updateStore.put(record);
            });
          }

          await new Promise(resolve => updateTransaction.oncomplete = resolve);
        } catch (error) {
          console.error("Error updating timestamp:", error);
        }

        // Get the newly stored episodes
        {
          const transaction = db.transaction("series_episodes", "readonly");
          const store = transaction.objectStore("series_episodes");
          const index = store.index("tmdbID");

          const newEpisodes = await new Promise((resolve, reject) => {
            const request = index.getAll(showId);
            request.onsuccess = () => resolve(request.result.filter(ep => ep.season_number === seasonNumber));
            request.onerror = () => reject(request.error);
          });

          renderEpisodeCards(newEpisodes, showId, seasonNumber);

          // scroll the season card into view
          setTimeout(() => {
            seasonCard.scrollIntoView({
              behavior: 'smooth',
              block: 'nearest',
              inline: 'start'
            });
          }, 350); // Small delay to ensure DOM has updated

          // Apply season filter after data is loaded
          await filterCreditsForSeason(showId, seasonNumber);
        }
      } else {
        showNotification(`No episode information available for Season ${seasonNumber}`, false);
      }
    } else {
      // Episodes already exist and don't need refresh
      renderEpisodeCards(episodeData, showId, seasonNumber);

      // scroll the season card into view
      setTimeout(() => {
        seasonCard.scrollIntoView({
          behavior: 'smooth',
          block: 'nearest',
          inline: 'start'
        });
      }, 350); // Small delay to ensure DOM has updated

      // Apply season filter after rendering
      await filterCreditsForSeason(showId, seasonNumber);
    }
  } catch (error) {
    console.error("Error loading episodes:", error);
    showNotification(`Error loading episodes: ${error.message}`, false);
  }
}

function generateEpisodeCards(episodes, seasonNumber) {
  // Determine the maximum number of digits needed for episode numbers
  const maxEpisodes = episodes.length;
  const padLength = maxEpisodes > 99 ? 3 : 2;

  return episodes.map(episode => {
    const stillUrl = episode.still_path
      ? `${tmdbImgBaseUrl}w300${episode.still_path}`
      : createPlaceholderSVG(300, 170, 'poster');

    const airDate = episode.air_date
      ? new Date(episode.air_date).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })
      : 'TBA';

    // Use dynamic padding based on the number of episodes
    const episodeCode = `s${seasonNumber.toString().padStart(2, '0')}e${episode.episode_number.toString().padStart(padLength, '0')}`;

    const tooltipContent = episode.overview
      ? `<div class="tooltip-content"><div class="tooltip-bio">${episode.overview}</div></div>`
      : `<div class="tooltip-content empty">No description available</div>`;

    return `
      <div class="card episode" id="s${seasonNumber}e${episode.episode_number}"
           data-type="episode" 
           data-season="${seasonNumber}" 
           data-episode="${episode.episode_number}"
           data-episode-code="${episodeCode}"
           data-show-id="${episode.tmdbID}">
        <div class="episode-still">
          ${episode.still_path
      ? `<img src="${stillUrl}" loading="lazy" alt="${episode.name}" class="media-image" />`
        : stillUrl
      }
        </div>
        <div class="card-title">${episode.name}</div>
        <div class="card-subtitle">${episodeCode}</div>
        <div class="card-year">${airDate}</div>
        <div class="card-rating">${episode.vote_average ? '⭐ ' + episode.vote_average.toFixed(1) : ''}</div>
        <div class="card-ttip episode" data-tt-pos="right">
          ${tooltipContent}
        </div>
      </div>
    `;
  }).join("");
}

function renderEpisodeCards(episodes, showId, seasonNumber) {
  if (!episodes || episodes.length === 0) {
    showNotification('No episodes available for this season', false);
    return;
  }

  // Sort episodes by episode number (ensure numeric sorting)
  episodes.sort((a, b) => Number(a.episode_number) - Number(b.episode_number));

  // Find the season card to insert episodes after
  const seasonCard = document.querySelector(`.card.season[data-season="${seasonNumber}"]`);
  if (!seasonCard) {
    console.error("Season card not found");
    return;
  }

  // Generate episode cards HTML
  const episodeCardsHTML = generateEpisodeCards(episodes, seasonNumber);

  // Create a temporary container to hold the HTML
  const temp = document.createElement('div');
  temp.innerHTML = episodeCardsHTML;

  // Get all episode elements
  const episodeElements = Array.from(temp.children);

  // Insert each episode card after the season card
  episodeElements.forEach(episode => {
    // Set the showId attribute
    episode.setAttribute('data-show-id', showId);

    // Add expanding animation class
    episode.classList.add('expanding');

    // Get actual episode number from the data attribute
    const episodeNum = parseInt(episode.getAttribute('data-episode'));

    // Insert after the season card or after the last episode with a lower number
    const previousEpisodes = Array.from(
      document.querySelectorAll(`.card.episode[data-season="${seasonNumber}"]`)
    ).filter(ep => parseInt(ep.getAttribute('data-episode')) < episodeNum);

    const lastPreviousEpisode = previousEpisodes.length > 0 ?
      previousEpisodes[previousEpisodes.length - 1] : null;

    if (lastPreviousEpisode) {
      lastPreviousEpisode.after(episode);
    } else {
      seasonCard.after(episode);
    }

    // Remove animation class after animation completes
    setTimeout(() => episode.classList.remove('expanding'), 300);
  });

  // Initialize tooltips for the new cards
  initCardTooltips();
}

async function filterCreditsForSeason(showId, seasonNum) {
  try {
    // Get all credits for this show
    const creditsStore = db.transaction("series_credits", "readonly").objectStore("series_credits");
    const showIndex = creditsStore.index("tmdbID");

    const allCredits = await new Promise((resolve, reject) => {
      const request = showIndex.getAll(showId);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });

    const seasonPrefix = `s${seasonNum.toString().padStart(2, '0')}e`;

    // Filter credits that include any episode from this season
    const seasonCredits = allCredits.filter(credit => {
      if (!credit.season_episodes) return false;

      if (Array.isArray(credit.season_episodes)) {
        return credit.season_episodes.some(ep => ep.startsWith(seasonPrefix));
      } else {
        // For backward compatibility with string format
        return credit.season_episodes.includes(seasonPrefix);
      }
    });

    // Get all personIDs for this season
    const seasonPersonIDs = new Set(seasonCredits.map(credit => credit.personID));

    // Hide all credit cards that don't belong to this season, tmdb data dependent which may be inaccurate ### check for better solution
    document.querySelectorAll('.card.person').forEach(card => {
      const personID = card.getAttribute('data-person-id');
      if (seasonPersonIDs.has(Number(personID))) {
        card.style.display = 'flex';
      } else {
        card.style.display = 'none';
      }
    });

    // Update UI to show we're filtering
    const castTitle = document.querySelector('.section-title[data-section="cast"]');
    const crewTitle = document.querySelector('.section-title[data-section="crew"]');

    if (castTitle) castTitle.textContent = `Cast for Season ${seasonNum}`;
    if (crewTitle) crewTitle.textContent = `Crew for Season ${seasonNum}`;
  } catch (error) {
    console.error("Error filtering credits for season:", error);
  }
}

async function filterCreditsForEpisode(showId, seasonNum, episodeNum) {
  try {
    // Get all credits for this show
    const creditsStore = db.transaction("series_credits", "readonly").objectStore("series_credits");
    const showIndex = creditsStore.index("tmdbID");

    const allCredits = await new Promise((resolve, reject) => {
      const request = showIndex.getAll(showId);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });

    const episodeCode = `s${seasonNum.toString().padStart(2, '0')}e${episodeNum.toString().padStart(2, '0')}`;

    // Filter credits that include this episode
    const episodeCredits = allCredits.filter(credit => {
      return credit.season_episodes &&
        (Array.isArray(credit.season_episodes) ?
          credit.season_episodes.includes(episodeCode) :
          credit.season_episodes.includes(episodeCode));
    });

    // Get all personIDs for this episode
    const episodePersonIDs = new Set(episodeCredits.map(credit => credit.personID));

    // Hide all credit cards that don't belong to this episode
    document.querySelectorAll('.card.person').forEach(card => {
      const personID = card.getAttribute('data-person-id');
      if (episodePersonIDs.has(Number(personID))) {
        card.style.display = 'flex';
      } else {
        card.style.display = 'none';
      }
    });

    // Update UI to show we're filtering
    const castTitle = document.querySelector('.section-title[data-section="cast"]');
    const crewTitle = document.querySelector('.section-title[data-section="crew"]');

    if (castTitle) castTitle.textContent = `Cast for ${episodeCode}`;
    if (crewTitle) crewTitle.textContent = `Crew for ${episodeCode}`;
  } catch (error) {
    console.error("Error filtering credits:", error);
  }
}

function resetCreditFilters() {
  // Show all credit cards
  document.querySelectorAll('.card.person').forEach(card => {
    card.style.display = 'flex';
  });

  // Reset section titles
  const castTitle = document.querySelector('.section-title[data-section="cast"]');
  const crewTitle = document.querySelector('.section-title[data-section="crew"]');

  if (castTitle) castTitle.textContent = 'Cast';
  if (crewTitle) crewTitle.textContent = 'Crew';
}

/**
 * Parses a string to extract the title and year.
 * @param {string} input - The input string (e.g., "Inception 2010").
 * @returns {Object} - An object containing `title` and `year` (if present).
 */
function parseTitleAndYear(input) {
  const regex = /(.*?)(?:\s(\d{4}))?$/; // Match title and optional year (e.g., "Inception 2010")
  const match = input.match(regex);

  return {
    title: match[1].trim(), // Extracted title
    year: match[2] ? parseInt(match[2], 10) : null, // Extracted year or null if not present
  };
}

// UI: helper to format big numbers
function formatAbbreviatedCurrency(num) {
  if (num === 0) return '$0';
  const absNum = Math.abs(num);
  if (absNum >= 1e9) {
    return '$' + (num / 1e9).toFixed(1).replace(/\.0$/, '') + 'B';
  } else if (absNum >= 1e6) {
    return '$' + (num / 1e6).toFixed(1).replace(/\.0$/, '') + 'M';
  } else if (absNum >= 1e3) {
    return '$' + (num / 1e3).toFixed(1).replace(/\.0$/, '') + 'K';
  } else {
    return '$' + num;
  }
}

// UI: helper to esacpe special characters
function escapeHTML(str) {
  return str.replace(/[&<>"']/g, function (match) {
    const escapeChars = {
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    };
    return escapeChars[match];
  });
}

function openVideoModal(videoKey, fromDetailView = false) {
  const overlay = document.getElementById("popup-overlay");
  const popup = document.getElementById("popup");
  const videoModal = document.getElementById("videoModal");
  const videoFrame = document.getElementById("videoFrame");
  const videoClose = document.querySelector(".close-modal[data-popup-type='video']");

  // Show overlay and popup
  overlay.classList.remove("hidden");
  popup.classList.remove("hidden");

  // Show video modal
  videoModal.classList.remove("hidden");
  videoModal.classList.add("active");

  // External view handling
  if (!fromDetailView) {
    videoModal.classList.add("external-view");
    popup.classList.add("external-view");
  } else {
    videoModal.classList.remove("external-view");
    popup.classList.remove("external-view");
  }

  // Set video source
  videoFrame.src = `https://www.youtube-nocookie.com/embed/${videoKey}?autoplay=1`;

  // Show close button initially, then set up auto-hide
  showCloseButton(videoClose);

  // Add mousemove handler to the video modal itself
  overlay.addEventListener("mousemove", handleVideoMouseMove);
}

function handleVideoMouseMove(event) {
  // Get close button within this modal
  const closeButton = event.currentTarget.querySelector(".close-modal[data-popup-type='video']");
  showCloseButton(closeButton);
}

function showCloseButton(button) {
  if (!button) return;

  // Clear any existing timeout
  clearTimeout(closeButtonTimeout);

  // Show the button
  button.classList.remove("fade");

  // Set timeout to hide it
  closeButtonTimeout = setTimeout(() => {
    button.classList.add("fade");
  }, 3600);
}

function isIOS() {
  const ua = navigator.userAgent;
  // Classic iOS devices
  if (/iPad|iPhone|iPod/.test(ua)) return true;
  // iPadOS masquerading as Mac
  if (
    ua.includes('Macintosh') &&
    typeof navigator.maxTouchPoints === 'number' &&
    navigator.maxTouchPoints > 1
  ) {
    return true;
  }
  return false;
}

async function checkVideoThumbnail(videoKey, mediaData, mediaType) {
  // Check memory cache first
  if (videoStatusCache.has(videoKey)) return videoStatusCache.get(videoKey);

  // Find the video object in the merged data
  const videoObj = mediaData.videos?.results?.find(v => v.key === videoKey);

  // Always filter out if status is 404 (on all platforms)
  if (videoObj?.status === 404) {
    videoStatusCache.set(videoKey, false);
    return false;
  }

  // On iOS: only use DB check for 404, skip network check
  if (isIOS()) {
    videoStatusCache.set(videoKey, true);
    return true;
  }

  try {
    // Simple GET request with no custom headers
    const response = await fetch(`https://img.youtube.com/vi/${videoKey}/hqdefault.jpg`);
    const isValid = response.status === 200;
    videoStatusCache.set(videoKey, isValid);

    // Update only the status in the appropriate details store
    const storePrefix = mediaType === 'tv' ? 'series' : 'movie';
    const storeName = `${storePrefix}_details`;
    const tmdbID = mediaData.tmdbID || mediaData.id;

    const transaction = db.transaction([storeName], "readwrite");
    const store = transaction.objectStore(storeName);

    const record = await new Promise(resolve => {
      const request = store.get(tmdbID);
      request.onsuccess = () => resolve(request.result);
    });

    if (record?.videos?.results) {
      const targetVideo = record.videos.results.find(v => v.key === videoKey);
      if (targetVideo) {
        targetVideo.status = response.status;
        await store.put(record);
      }
    }

    // Update the status in the merged data object for consistency
    if (videoObj) videoObj.status = response.status;

    // Only update the card UI if the video is invalid (404)
    if (!isValid) {
      updateMediaCard(mediaData.imdbID, mediaType);
    }

    return isValid;
  } catch (error) {
    console.error('Error checking video thumbnail:', error);
    videoStatusCache.set(videoKey, false);

    // Update status to 404 for errors, only in the appropriate store
    const storePrefix = mediaType === 'tv' ? 'series' : 'movie';
    const storeName = `${storePrefix}_details`;
    const tmdbID = mediaData.tmdbID || mediaData.id;

    const transaction = db.transaction([storeName], "readwrite");
    const store = transaction.objectStore(storeName);

    const record = await new Promise(resolve => {
      const request = store.get(tmdbID);
      request.onsuccess = () => resolve(request.result);
    });

    if (record?.videos?.results) {
      const targetVideo = record.videos.results.find(v => v.key === videoKey);
      if (targetVideo) {
        targetVideo.status = 404;
        await store.put(record);
      }
    }

    // Update the status in the merged data object for consistency
    if (videoObj) videoObj.status = 404;

    // Update the card UI since we have an error (treated as 404)
    updateMediaCard(mediaData.imdbID, mediaType);

    return false;
  }
}

/**
 * Initializes UI and functionality based on loaded user settings
 */
function initializeUserSettings() {
  console.log("Initializing UI based on loaded settings...");

  if (!userSettings || !userSettings.username) {
    console.error("User settings not loaded properly!");
    return;
  }

  // Update API key manager with current settings
  apiKeyManager.initialize(userSettings);

  // Apply settings to UI
  updateUserIconDisplay();
  initializeGridSize(); 

  console.log("User settings applied to UI.");
}

/**
 * Update the user icon in the header
 */
function updateUserIconDisplay() {
  const userIconElement = document.getElementById("user-icon");
  if (!userIconElement) return;

  // Check if we have a custom user icon in settings
  if (userSettings.user_icon) {
    userIconElement.innerHTML = `<img src="${userSettings.user_icon}" alt="User Icon" class="icon-svg">`;
  } else {
    userIconElement.innerHTML = `<svg class="icon-svg"><use href="#user_default"/></svg>`;
  }
}

/**
 * Get user settings from the database directly
 * @param {string} [username] - Username to get settings for, defaults to current user
 * @returns {Promise<Object>} User settings object
 */
async function getUserSettingsFromDB(username = null) {
  return new Promise((resolve) => {
    if (!db) {
      console.error("Database not available");
      return resolve(null);
    }

    try {
      const currentUser = username || (userSettings?.username);
      if (!currentUser) {
        return resolve(null);
      }

      const transaction = db.transaction(['user_settings'], 'readonly');
      const store = transaction.objectStore('user_settings');
      const request = store.get(currentUser);

      request.onsuccess = () => resolve(request.result || null);
      request.onerror = (event) => {
        console.error("Error getting user settings:", event.target.error);
        resolve(null);
      };
    } catch (error) {
      console.error("Error in getUserSettingsFromDB:", error);
      resolve(null);
    }
  });
}

/**
 * Save user settings to the database.
 * If fieldsToUpdate is provided, only those fields will be updated in the DB.
 * @param {Object} settings - Settings object to save (must include username).
 * @param {Array<string>} [fieldsToUpdate] - Optional array of fields to update.
 * @returns {Promise<boolean>} Success or failure.
 */
async function saveUserSettingsToDB(settings, fieldsToUpdate = null) {
  if (!db || !settings || !settings.username) {
    console.error("Cannot save settings: Missing DB or invalid settings");
    return false;
  }

  // PARTIAL UPDATE: Only update specified fields
  if (fieldsToUpdate && Array.isArray(fieldsToUpdate) && fieldsToUpdate.length > 0) {
    const transaction = db.transaction(['user_settings'], 'readwrite');
    const store = transaction.objectStore('user_settings');
    const getRequest = store.get(settings.username);

    return new Promise((resolve) => {
      getRequest.onsuccess = (event) => {
        const existingSettings = event.target.result || {};
        for (const field of fieldsToUpdate) {
          if (field in settings) {
            existingSettings[field] = settings[field];
          }
        }
        const putRequest = store.put(existingSettings);
        putRequest.onsuccess = () => {
          if (userSettings?.username === existingSettings.username) {
            Object.assign(userSettings, existingSettings);
          }
          resolve(settings.username);
        };
        putRequest.onerror = (event) => {
          console.error("Error saving user settings:", event.target.error);
          resolve(false);
        };
      };
      getRequest.onerror = (event) => {
        console.error("Error fetching user settings:", event.target.error);
        resolve(false);
      };
    });
  }

  // FULL SAVE: Merge with existing settings to avoid deleting fields
  try {
    const transaction = db.transaction(['user_settings'], 'readwrite');
    const store = transaction.objectStore('user_settings');
    const getRequest = store.get(settings.username);

    return await new Promise((resolve) => {
      getRequest.onsuccess = (event) => {
        const existingSettings = event.target.result || {};
        // Merge: new settings overwrite old, but keep missing fields
        const mergedSettings = { ...existingSettings, ...settings };
        const putRequest = store.put(mergedSettings);

        putRequest.onsuccess = () => {
          if (userSettings?.username === mergedSettings.username) {
            userSettings = mergedSettings;
          }
          resolve(settings.username);
        };
        putRequest.onerror = (event) => {
          console.error("Error saving user settings:", event.target.error);
          resolve(false);
        };
      };
      getRequest.onerror = (event) => {
        console.error("Error fetching user settings:", event.target.error);
        resolve(false);
      };
    });
  } catch (error) {
    console.error("Error in saveUserSettingsToDB:", error);
    return false;
  }
}

/**
 * UI: Detail View - Toggle custom backdrop 
 * @param {*} imdbID 
 * @param {*} filePath 
 * @param {*} defaultFilePath 
 * @returns 
 */
function updateBackdropToggleState(imdbID, filePath, defaultFilePath) {
  if (!imdbID) {
    console.error("updateBackdropToggleState: imdbID is missing");
    return;
  }
  const toggleBackdropBtn = document.getElementById("toggle-active-backdrop");
  if (!toggleBackdropBtn) return;

  const activeBackdrop = getMediaCustomization(imdbID, 'backdrop');
  const normalize = s => s ? s.trim().toLowerCase() : "";

  // Determine if the currently displayed image is the default image.
  const isDefaultImage = normalize(filePath) === normalize(defaultFilePath);
  // Determine if the currently viewed image matches the stored custom backdrop.
  const isActiveBackdrop = activeBackdrop && normalize(filePath) === normalize(activeBackdrop);

  if (!activeBackdrop) {
    // No custom backdrop is set.
    if (isDefaultImage) {
      // We're viewing the default image and no override exists.
      toggleBackdropBtn.textContent = "(Default Backdrop)";
      toggleBackdropBtn.disabled = true;
      toggleBackdropBtn.onclick = null;
    } else {
      // Viewing a non‐default image; allow user to override.
      toggleBackdropBtn.textContent = "Set as Active Backdrop";
      toggleBackdropBtn.disabled = false;
      toggleBackdropBtn.onclick = function () {
        // Save current filePath as the custom backdrop.
        setMediaCustomization(imdbID, "backdrop", filePath);
        updateDetailBackdrop(imdbID); // Now the popup shows the custom backdrop.
        updateBackdropToggleState(imdbID, filePath, defaultFilePath);
      };
    }
  } else {
    // A custom backdrop is stored.
    if (isDefaultImage) {
      // Viewing the default image while a custom backdrop exists:
      // Clicking will remove the custom setting.
      toggleBackdropBtn.textContent = "Set as Active Backdrop";
      toggleBackdropBtn.disabled = false;
      toggleBackdropBtn.onclick = function () {
        clearMediaCustomization(imdbID, "backdrop");
        updateDetailBackdrop(imdbID, defaultFilePath);
        updateBackdropToggleState(imdbID, filePath, defaultFilePath);
      };
    } else if (isActiveBackdrop) {
      // The user is viewing the image that is the current custom backdrop.
      // On click, remove the custom override, update the popup DOM to show the default backdrop,
      // but continue to pass the original custom image filePath into the toggle updater.
      toggleBackdropBtn.textContent = "Revert to Default";
      toggleBackdropBtn.disabled = false;
      toggleBackdropBtn.onclick = function () {
        // Remove the custom override.
        clearMediaCustomization(imdbID, "backdrop");
        // Update the popup DOM to the default backdrop.
        updateDetailBackdrop(imdbID, defaultFilePath);
        // Continue to show (if still viewing the custom image) the button as clickable
        // and labeled "Set as Active Backdrop" since the custom override is now removed.
        updateBackdropToggleState(imdbID, filePath, defaultFilePath);
      };
    } else {
      // Viewing an image that is neither the default nor the custom active backdrop, so allow overriding.
      toggleBackdropBtn.textContent = "Set as Active Backdrop";
      toggleBackdropBtn.disabled = false;
      toggleBackdropBtn.onclick = function () {
        setMediaCustomization(imdbID, "backdrop", filePath);
        updateDetailBackdrop(imdbID); // Update popup to new custom image.
        updateBackdropToggleState(imdbID, filePath, defaultFilePath);
      };
    }
  }
}

// Updates the backdrop shown in the popup DOM.
// When an overridePath is provided it uses that (typically the default file path),
// otherwise it looks for the active custom backdrop in local storage.
function updateDetailBackdrop(imdbID, overridePath) {
  const popup = document.getElementById("popup");
  if (!popup) return;
  let activeFilePath = typeof overridePath === "string" ? overridePath : getMediaCustomization(imdbID, 'backdrop');
  // Fall back to the movie’s default backdrop from the cache, if available.
  const media = mediaCache.get(imdbID);
  if (!activeFilePath && media) {
    activeFilePath = media.defaultBackdrop || "";
  }
  popup.style.setProperty("--detail-backdrop", activeFilePath ? `url(${buildTMDbImageUrl(activeFilePath, "backdrop")})` : "none");
}

async function initializeApplication() {
  try {
    // 1. Open DB
    db = await openDB();
    // console.log("Database opened.");

    // Set up essential components that don't depend on user settings
    registerServiceWorker();
    setupFileInputHandlers();
    setupInputChangeHandlers();
    setupSearchInputHandlers();
    setupDropdowns();
    setupPanelHoverHandlers();
    initializeRatingsOnVisibility();

    // 2. Load core settings and list configurations, check setup status
    const coreDataStatus = await loadCoreAppData(db);
    // console.log("Core app data load attempt complete.");

    // 3. Conditional Initialization
    if (coreDataStatus.setupNeeded) {
      console.log("Setup needed, showing settings popup.");
      // Show popup, passing the function to run AFTER first profile is saved
      showSettingsPopup(completeInitialization);
    } else {
      // console.log("Setup not needed, proceeding with initialization.");
      // Run the rest of the initialization immediately
      await completeInitialization();
    }

  } catch (error) {
    console.error("CRITICAL ERROR during application initialization:", error);
    displayInitializationError(error);
  }
}

async function completeInitialization() {
  // console.log("Running completeInitialization...");
  if (!userSettings || userSettings.requiresSetup) {
    console.error("Attempted to complete initialization, but setup is still required or settings are missing.");
    showNotification("⚠️ Please complete profile setup first.", false);
    return;
  }

  // Add this check to ensure API keys are present
  if (!userSettings.tmdb_apikey) {
    console.log("TMDB API key not yet configured, waiting for user to enter it");
    showNotification("⚠️ Please enter your TMDB API key in settings.", false);
    return;
  }

  // Add check for at least one OMDB API key
  if (!userSettings.omdb_apikeys || !userSettings.omdb_apikeys.length) {
    console.log("OMDB API keys not yet configured, waiting for user to enter at least one");
    showNotification("⚠️ Please enter at least one OMDB API key in settings.", false);
    return;
  }

  // Ensure DB is available
  if (!db) {
    console.error("DB not available for completeInitialization");
    displayInitializationError(new Error("Database connection lost before initialization could complete."));
    return;
  }

  // Initialize components that DEPEND on settings/lists
  initializeUserSettings(); // Apply loaded appSettings to UI
  await loadMediaSettings();
  await initializeIcons(); // Uses loaded listsMap
  initializeListFilters(); // Uses listsMap

  // Load media data and initialize UI elements
  await loadRecords(db);
  await populateGenres(db);
  await loadListStates();
  initializeGenreFilter();
  initializeYearSlider();
  initializeNavPanel();

  //buildLetterIndex(); // build on first jump

  console.log("Application initialization complete.");
}

// Media action handlers
async function handleDetailView(event, element) {
  event.preventDefault();
  event.stopPropagation();

  // Find the closest media card and its imdbID
  const mediaCard = element.closest(".media-card");
  if (!mediaCard || !mediaCard.dataset.imdbid) {
    console.error("Media card or imdbID not found.");
    return showNotification("⚠️ Title selection invalid", false);
  }

  const imdbID = mediaCard.dataset.imdbid;
  const mediaType = mediaCard.dataset.mediaType || 'movie';

  // Retrieve the media data from the global mediaCache
  const mediaData = mediaCache.get(imdbID);
  if (!mediaData) {
    console.error("Record not found in cache for imdbID:", imdbID);
    return showNotification("⚠️ Unable to load record details", false);
  }

  // Load additional media data from IndexedDB using its tmdbID.
  try {
    const { details, credits, peopleMap, keywords } = await loadMediaData(mediaData.tmdbID, mediaType);

    if (details) {
      let fieldsToMerge = [
        // Common fields
        "genres", "overview", "production_companies", "recommendations",
        "similar", "videos", "tagline", "images"
      ];

      // Add media type specific fields
      if (mediaType === 'movie') {
        fieldsToMerge.push(
          "belongs_to_collection",
          "budget",
          "revenue"
        );
      } else if (mediaType === 'tv') {
        fieldsToMerge.push(
          "created_by",
          "in_production",
          "last_air_date",
          "last_episode_to_air",
          "networks",
          "next_episode_to_air",
          "type",
          "original_name"
        );
      }

      // Merge the fields into mediaData
      fieldsToMerge.forEach(field => {
        if (details[field] !== undefined) {
          // Special handling for networks to ensure array format
          if (field === 'networks' && !Array.isArray(details[field])) {
            mediaData[field] = [details[field]];
          } else {
            mediaData[field] = details[field];
          }
        }
      });

      // Additional processing for TV series creators
      if (mediaType === 'tv' && details.created_by) {
        mediaData.series_details = {
          created_by: details.created_by.map(creator => ({
            id: creator.id,
            name: creator.name,
            profile_path: creator.profile_path,
            credit_id: creator.credit_id
          }))
        };
      }

      if (mediaData.videos && mediaData.videos.results) {
        const checkResults = await Promise.all(
          mediaData.videos.results.map(async video => {
            if (video.site !== "YouTube") return true;
            if (video.status === 200) return true;
            if (video.status === 404) return false;

            return await checkVideoThumbnail(video.key, mediaData, mediaType);
          })
        );

        mediaData.videos.results = mediaData.videos.results.filter((_, index) => checkResults[index]);
      }

      // Check if the movie belongs to a collection; if so, fetch its data.
      if (details.belongs_to_collection && details.belongs_to_collection.id) {
        mediaData.belongs_to_collection = details.belongs_to_collection;

        const collectionUrl = `${tmdbApiBaseUrl}collection/${details.belongs_to_collection.id}?api_key=${apiKeyManager.getTmdbKey()}&language=en-US`;
        try {
          const collectionData = await queryTMDBSimple(collectionUrl);
          // Assign the collection movies data (from `parts`) to movieData.collection
          mediaData.collection = Array.isArray(collectionData.parts) ? collectionData.parts : [];
          // Sort the collection by release_date
          if (mediaData.collection.length) {
            mediaData.collection.sort((a, b) => {
              const dateA = new Date(a.release_date);
              const dateB = new Date(b.release_date);
              return dateA - dateB; // Sorts in ascending order
            });
          }
        } catch (err) {
          console.error("Error fetching collection movies:", err);
        }
      }
    }

    // Generate the detail view HTML using the updated movieData and the credits/people map.
    const detailHTML = generateDetailViewHtml(mediaData, credits, peopleMap, keywords);

    // Fetch the existing popup overlay and popup elements
    const overlay = document.getElementById("popup-overlay");
    const popup = document.getElementById("popup");

    const contentContainer = popup.querySelector(".popup-content");

    if (!contentContainer) {
      console.error("Popup content container not found!");
      return;
    }

    // Populate the popup and display the overlay
    contentContainer.innerHTML = detailHTML;
    popup.classList.add("detail-view");
    // Display the overlay and popup immediately
    overlay.classList.remove("hidden");
    popup.classList.remove("hidden");

    setTimeout(initCardTooltips, 100);

    // Use requestAnimationFrame to trigger the panel's show transition
    requestAnimationFrame(() => {
      popup.classList.add("show");
    });

    // Preload the backdrop image in parallel
    if (mediaData.backdrop_path && mediaData.backdrop_path !== "N/A") {
      const backdropUrl = `${tmdbImgBaseUrl}w1280${mediaData.backdrop_path}`;
      const tempImg = new Image();
      tempImg.onload = () => {
        // Update the custom CSS property with the loaded image URL
        popup.style.setProperty("--detail-backdrop", `url(${backdropUrl})`);
        popup.style.setProperty("--detail-gradient", "linear-gradient(to right, rgba(13, 13, 13, 1) 0%, rgba(13, 13, 13, 0.8) 25%, rgba(13, 13, 13, 0.5) 50%, transparent 100%)");
        popup.classList.add("loaded");
      };
      tempImg.onerror = () => {
        // Fallback: if image fails, clear the backdrop while keeping the panel visible
        popup.style.setProperty("--detail-backdrop", "none");
        popup.style.setProperty("--detail-gradient", "none");
      };
      tempImg.src = backdropUrl;
    } else {
      popup.style.setProperty("--detail-backdrop", "none");
      popup.style.setProperty("--detail-gradient", "none");
    }

    document.body.style.overflow = 'hidden';
  } catch (error) {
    console.error(error);
    showNotification("⚠️ Error loading details", false);
  }
}

async function handleDeleteMedia(_event, element) {
  try {
    const { imdbID, mediaType, title } = getMediaInfo(element, 'delete');

    showCustomConfirm(
      `<h4>Are you sure you want to delete this record?</h4><h2>"${title}"</h2>`,
      async () => {
        await deleteRecord(imdbID, mediaType, db);
        showNotification(`❌ Entry deleted!`, false);
      }
    );
  } catch (error) {
    showNotification(`⚠️ ${error.message}`, false);
  }
}

async function handleRefreshMedia(_event, element) {
  try {
    // Get both imdbID and tmdbID if available
    const { imdbID, mediaType, tmdbID } = getMediaInfo(element, 'refresh');
    
    // Determine category based on mediaType
    const category = mediaType === 'tv' ? 'series' : 'movie';
    
    // Call addTitle with the refresh flag and tmdbID
    await addTitle(db, { 
      imdb_ID: imdbID, 
      tmdbID: Number(tmdbID),
      category: category,
      isRefresh: true 
    });
    
  } catch (error) {
    showNotification(`⚠️ Refresh failed: ${error.message}`, false);
  }
}

function handleDropdownSelection(_event, element) {
  const dropdownMenu = element.closest('.dropdown-menu, .dropdown-menu-country');
  if (!dropdownMenu) return;

  const dropdownId = dropdownMenu.id;
  const value = element.getAttribute('data-value');

  // Update active state
  dropdownMenu.querySelectorAll('.active').forEach(item => item.classList.remove('active'));
  element.classList.add('active');

  // Hide the dropdown for all other cases
  dropdownMenu.classList.add('hidden');

  // Dispatch based on dropdown ID
  switch (dropdownId) {
    case 'dropdown-menu-search': {
      const selectedOption = document.getElementById('selected-option');
      if (selectedOption) selectedOption.textContent = value;
      handleMediaTypeSelection(value);
      break;
    }
    case 'dropdown-menu-country': {
      const selectedOption = document.getElementById('selected-option-country');
      if (selectedOption) selectedOption.textContent = value;
      handleCountrySelection(value);
      break;
    }
    default:
      // Handle any future dropdowns
      break;
  }
}

function handleMediaTypeSelection(value) {
  if (value === "Movies") {
    activeMediaType = "movie";
  } else if (value === "Series") {
    activeMediaType = "tv";
  } else {
    activeMediaType = null;
  }

  if (activeMediaType !== null || (activeMediaType === null && previousMediaType !== null)) {
    applyFiltersAndSearch(true, null);
    previousMediaType = activeMediaType;
  }

  // Trigger search if there's a query
  const searchInput = document.getElementById("search-input");
  if (searchInput.value.trim()) {
    searchInput.dispatchEvent(new Event("input", { bubbles: true }));
  }
}

function handleCountrySelection(value) {
  activeCountry = value;
  applyFiltersAndSearch();
  updateFilterButtonState(document.getElementById("dropdown-button-country"));
}

function toggleDropdown(event, element) {
  event.preventDefault(); // Prevent default button behavior
  event.stopPropagation(); // Prevent event from bubbling up

  const dropdownId = element.getAttribute('data-dropdown');
  const dropdownMenu = document.getElementById(dropdownId);

  if (dropdownMenu) {
    // Close all other dropdowns
    document.querySelectorAll('.dropdown-menu').forEach(menu => {
      if (menu.id !== dropdownId) {
        menu.classList.add('hidden');
      }
    });

    // Toggle this dropdown
    dropdownMenu.classList.toggle("hidden");
  } else {
    console.error(`Dropdown menu with ID "${dropdownId}" not found`);
  }
}

function closeDropdownsOutsideClick(event) {
  const clickedOnDropdown = event.target.closest('[data-dropdown]');
  const clickedInDropdown = event.target.closest('.dropdown-menu, .dropdown-menu-country, .customlist-dropdown-panel');

  // Only close if NOT inside any dropdown or the custom panel
  if (!clickedOnDropdown && !clickedInDropdown) {
    document.querySelectorAll('.dropdown-menu, .dropdown-menu-country').forEach(dropdown => {
      dropdown.classList.add('hidden');
    });
    // Also close the custom list panel if open
    const customPanel = document.getElementById('customlist-dropdown-panel');
    if (customPanel && !customPanel.classList.contains('hidden')) {
      customPanel.classList.add('hidden');
      updateFilterButtonState(document.getElementById('customlist-filter-icon'));
    }
  }
}

function setupDropdowns() {
  const dropdownButtons = document.querySelectorAll('[data-dropdown]');

  dropdownButtons.forEach(button => {
    // Get the dropdown element
    const dropdownId = button.getAttribute('data-dropdown');
    const dropdown = document.getElementById(dropdownId);

    if (!dropdown) {
      console.error(`Dropdown element not found for ID: ${dropdownId}`);
      return;
    }

    // Ensure dropdown starts hidden
    if (!dropdown.classList.contains('hidden')) {
      dropdown.classList.add('hidden');
    }

    // Set proper ARIA attributes for accessibility
    dropdown.setAttribute('aria-hidden', 'true');
    button.setAttribute('aria-expanded', 'false');
    button.setAttribute('aria-controls', dropdownId);

    // Attach click/toggle logic
    button.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();

      // Hide all other dropdowns
      document.querySelectorAll('.dropdown-menu').forEach(menu => {
        if (menu !== dropdown) {
          menu.classList.add('hidden');
          menu.setAttribute('aria-hidden', 'true');
        }
      });

      // Toggle this dropdown
      const isHidden = dropdown.classList.contains('hidden');
      dropdown.classList.toggle('hidden');
      dropdown.setAttribute('aria-hidden', isHidden ? 'false' : 'true');
      button.setAttribute('aria-expanded', isHidden ? 'true' : 'false');
    });
  });

}

function handleLogoClick(event) {
  event.preventDefault();
  
  if (window.scrollY > 50 && letterIndex && letterIndex.size > 0) {
    // If scrolled down significantly and letter index exists
    const firstKey = letterIndex.keys().next().value;
    
    // Check if we're already at the first letter and index 0
    if (currentJumpLetter !== firstKey || currentJumpIndex !== 0) {
      // Jump to first letter if we're not already there
      throttledJumpToLetter(firstKey);
    } else {
      // Just scroll to top if we're already at the first letter
      window.scrollTo({
        top: 0,
        behavior: 'smooth'
      });
    }
  } else {
    // Always scroll to top when logo is clicked, even if we're on the first page
    window.scrollTo({
      top: 0,
      behavior: 'smooth'
    });
  }
}

function handleSettingsClick() {
  try {
    showSettingsPopup();
  } catch (error) {
    console.error("Error calling Settings:", error);
    showNotification("An error occurred during the settings process.", false);
  }
}

function handleAboutClick() {
  try {
    showAboutPopup();
  } catch (error) {
    console.error("Error calling About Popup", error);
    showNotification("An error occurred during the About Popup process.", false);
  }
}

function handleBackupClick() {
  try {
    backupDatabase(db);
  } catch (error) {
    console.error("Error during backup:", error);
    showNotification("An error occurred during the backup process.", false);
  }
}

function handleAddEntry() {
  const searchInput = document.getElementById("search-input").value.trim();
  const selectedCategory = document.getElementById("selected-option").textContent;

  if (!searchInput) {
    console.error("Search input is empty.");
    return;
  }

  const { title, year } = parseTitleAndYear(searchInput);
  const normalizedTitle = normalizeString(title);

  let mediaType;
  switch (selectedCategory) {
    case "Movies": mediaType = "movie"; break;
    case "Series": mediaType = "series"; break;
    case "All": mediaType = "all"; break;
    default: mediaType = "movie";
  }

  addTitle(db, { title: normalizedTitle, year, category: mediaType });
}

function toggleListFilter(_event, element) {
  const listName = element.dataset.listName || element.id.replace('-filter-icon', '');
  const listConfig = getListConfig(listName);
  const type = listConfig.type || 'core';

  const isActive = activeLists.some(item => item.name === listName && item.type === type);

  if (isActive) {
    activeLists = activeLists.filter(item => !(item.name === listName && item.type === type));
    element.classList.remove("active");
  } else {
    activeLists.push({ name: listName, type });
    element.classList.add("active");
  }

  applyFiltersAndSearch();

  // Force blur to remove focus/hover state on touch devices
  element.blur();
}

async function toggleListMembership(imdbID, listName, username) {
  if (!username || !imdbID || !listName) {
    throw new Error('Missing required parameters for list toggle');
  }

  const listConfig = getListConfig(listName);
  const listType = listConfig.type || 'core';
  const media = mediaCache.get(imdbID);
  
  if (!media) throw new Error(`Media not found for ID: ${imdbID}`);
  
  // Determine new state (optimistic update)
  if (!media.lists) media.lists = {};
  const newState = !media.lists[listName];
  
  // Update in-memory state immediately
  media.lists[listName] = newState;
  
  try {
    const transaction = db.transaction(['lists', 'lists_metadata'], 'readwrite');
    const listStore = transaction.objectStore('lists');
    const metadataStore = transaction.objectStore('lists_metadata');

    // Make sure we're using the same structure for both operations
    const keyPathValues = { username, imdbID, type: listType, list: listName };

    // First, ensure the list metadata exists
    const metadataKey = [username, listType, listName];
    const metadataRequest = metadataStore.get(metadataKey);

    // Wait for metadata check to complete
    const metadataExists = await new Promise((resolve, reject) => {
      metadataRequest.onsuccess = () => resolve(!!metadataRequest.result);
      metadataRequest.onerror = () => reject(metadataRequest.error);
    });

    // If metadata doesn't exist and it's a core list, create it
    if (!metadataExists && listType === 'core') {
      // Core list creation logic (unchanged from original)
      console.log(`Creating core list "${listName}" for user "${username}"`);
      
      const defaultTemplate = listsMap[listName] || iconListConfig[listName] || {
        symbolId: listName,
        label: listName.charAt(0).toUpperCase() + listName.slice(1),
        description: `Your ${listName} items`
      };

      const newMetadata = {
        username,
        type: 'core',
        list: listName,
        symbolId: defaultTemplate.symbolId || listName,
        label: defaultTemplate.label || listName.charAt(0).toUpperCase() + listName.slice(1),
        description: defaultTemplate.description || `Your ${listName} items`,
        created: Date.now(),
        lastUpdated: Date.now(),
        isPublic: true,
        ratings: {},
        ratingsCount: 0,
        avgRating: 0,
        tags: ['core']
      };

      // Add metadata to the database
      await new Promise((resolve, reject) => {
        const addRequest = metadataStore.add(newMetadata);
        addRequest.onsuccess = resolve;
        addRequest.onerror = (event) => {
          console.error("Failed to create core list metadata:", event.target.error);
          reject(event.target.error);
        };
      });

      // Update listsMap to include the newly created core list
      listsMap[listName] = newMetadata;
    } else if (!metadataExists && listType === 'custom') {
      throw new Error("Custom list does not exist. Please create it first.");
    }

    // Now proceed with item toggle
    const getRequest = listStore.get([keyPathValues.username, keyPathValues.imdbID,
      keyPathValues.type, keyPathValues.list]);

    return new Promise((resolve, reject) => {
      getRequest.onsuccess = async () => {
        try {
          const existingRecord = getRequest.result;

          if (existingRecord) {
            // Remove from list
            await new Promise((res, rej) => {
              const deleteRequest = listStore.delete([keyPathValues.username, keyPathValues.imdbID,
                keyPathValues.type, keyPathValues.list]);
              deleteRequest.onsuccess = res;
              deleteRequest.onerror = rej;
            });
          } else {
            // Add to list
            const record = {
              username: keyPathValues.username,
              imdbID: keyPathValues.imdbID,
              type: keyPathValues.type,
              list: keyPathValues.list,
              dateAdded: new Date().toISOString(),
              mediaTitle: media?.Title || media?.name || 'Unknown',
              mediaType: media?.mediaType || "movie"
            };

            await new Promise((res, rej) => {
              const addRequest = listStore.add(record);
              addRequest.onsuccess = res;
              addRequest.onerror = event => {
                console.error("Add error:", event.target.error);
                rej(event.target.error);
              };
            });
          }

          // Update metadata counts
          await updateListMetadataStats(listName, listType, username);
          
          // No need to update the cache again since we did it optimistically
          resolve(newState);
        } catch (err) {
          // Revert the optimistic update on error
          media.lists[listName] = !newState;
          reject(err);
        }
      };

      getRequest.onerror = () => reject(getRequest.error);
    });
  } catch (error) {
    // Revert the optimistic update on error
    media.lists[listName] = !newState;
    console.error('Database error during list toggle:', error);
    throw error;
  }
}

// Helper function to update list metadata counts
async function updateListMetadataStats(listName, listType, username) {
  try {
    const transaction = db.transaction(['lists', 'lists_metadata'], 'readwrite');
    const listsStore = transaction.objectStore('lists');
    const metadataStore = transaction.objectStore('lists_metadata');

    // Count items in list
    const index = listsStore.index('username_list');
    const keyRange = IDBKeyRange.only([username, listName]);
    const countRequest = index.count(keyRange);

    return new Promise((resolve, reject) => {
      countRequest.onsuccess = async () => {
        try {
          const count = countRequest.result;

          // Get metadata record
          const metadataKey = [username, listType, listName];
          const getRequest = metadataStore.get(metadataKey);

          getRequest.onsuccess = async () => {
            try {
              let metadata = getRequest.result;

              if (metadata) {
                // Update existing record
                metadata.itemCount = count;
                metadata.lastUpdated = new Date().toISOString();

                await new Promise((res, rej) => {
                  const updateRequest = metadataStore.put(metadata);
                  updateRequest.onsuccess = res;
                  updateRequest.onerror = rej;
                });

                // Update the cached listsMap
                if (listsMap[listName]) {
                  listsMap[listName].itemCount = count;
                  listsMap[listName].lastUpdated = metadata.lastUpdated;
                }
              }

              resolve();
            } catch (err) {
              reject(err);
            }
          };

          getRequest.onerror = () => reject(getRequest.error);
        } catch (err) {
          reject(err);
        }
      };

      countRequest.onerror = () => reject(countRequest.error);
    });
  } catch (error) {
    console.error('Error updating list stats:', error);
    // Non-critical, don't throw
  }
}

function cycleWatchedFilter(_event, element) {
  const currentState = element.dataset.state || "none";
  let newState;

  if (currentState === "none") {
    newState = "watched";
  } else if (currentState === "watched") {
    newState = "unwatched";
  } else {
    newState = "none";
  }

  element.dataset.state = newState;
  updateWatchedIcon(newState);
  updateActiveListsForWatched(newState);
  applyFiltersAndSearch();
}

// Year sort toggle function
function toggleYearSort() {
  const yearFilterButton = document.getElementById('year-filter-button');
  const ratingsFilterButton = document.getElementById('ratings-filter-button');

  // Reset rating sort state 
  ratingsFilterButton.classList.remove('sort-active', 'sort-asc', 'sort-desc');
  document.querySelectorAll('.rating-filter, .rating-filter-icon').forEach(element => {
    element.classList.remove('sort-asc', 'sort-desc');
  });
  
  // Reset the current sort field if it was a rating type
  if (currentSortField === 'imdb' || currentSortField === 'tmdb' || 
      currentSortField === 'metascore' || currentSortField === 'rt' || 
      currentSortField === 'weighted') {
    currentSortField = "year"; // We're now sorting by year
  }

  if (!yearFilterButton.classList.contains('sort-asc') && !yearFilterButton.classList.contains('sort-desc')) {
    // Not sorted by year yet - switch to ascending
    filteredRecords = sortRecords(filteredRecords, "year", true);
    yearFilterButton.classList.add('sort-active', 'sort-asc');
    yearFilterButton.classList.remove('sort-desc');
    currentSortField = "year";
    yearSortAscending = true;
    showNotification('Sorting by year (ascending)', false);
  } else if (yearFilterButton.classList.contains('sort-asc')) {
    // Already sorted ascending - switch to descending
    filteredRecords = sortRecords(filteredRecords, "year", false);
    yearFilterButton.classList.add('sort-active', 'sort-desc');
    yearFilterButton.classList.remove('sort-asc');
    currentSortField = "year";
    yearSortAscending = false;
    showNotification('Sorting by year (descending)', false);
  } else {
    // Already sorted descending - switch to A-Z
    filteredRecords = sortRecords(filteredRecords, "title", true);
    currentSortField = "title";
    yearFilterButton.classList.remove('sort-active', 'sort-asc', 'sort-desc');
    showNotification('Sorting by title (A-Z)', false);
  }

  // Reset pagination variables without clearing the grid
  startIndex = 0;
  endIndex = 0;
  renderedMediaIDs.clear();
  loadPaginatedRecords(startIndex);
  updateMatchCount();
}

function handleYearFilterButtonClick() {

    toggleYearSort();
  
}

function setupPanelHoverHandlers() {
  const panelConfigs = [
    { button: '#genre-filter-button', panel: '#genre-panel' },
    { button: '#year-filter-button', panel: '#year-filter-panel' },
    { button: '#ratings-filter-button', panel: '#ratings-filter-panel' }
  ];

  let hideTimeout;
  let openPanel = null;

  panelConfigs.forEach(({ button, panel }) => {
    const buttonEl = document.querySelector(button);
    const panelEl = document.querySelector(panel);
    if (!buttonEl || !panelEl) return;
    if (buttonEl.disabled) return;

    // Desktop: hover
    if (!isTouchDevice()) {
      buttonEl.addEventListener('mouseover', () => {
        clearTimeout(hideTimeout);
        panelConfigs.forEach(cfg => {
          const otherPanel = document.querySelector(cfg.panel);
          if (otherPanel && cfg.panel !== panel) otherPanel.classList.add('hidden');
        });
        panelEl.classList.remove('hidden');
        openPanel = panelEl;
        if (panel === '#ratings-filter-panel') {
          initializeRatingsOnVisibility();
        }
      });

      buttonEl.addEventListener('mouseleave', () => {
        hideTimeout = setTimeout(() => {
          if (!panelEl.matches(':hover')) panelEl.classList.add('hidden');
        }, 90);
      });

      panelEl.addEventListener('mouseleave', () => {
        hideTimeout = setTimeout(() => {
          if (!buttonEl.matches(':hover')) panelEl.classList.add('hidden');
        }, 400);
      });

      panelEl.addEventListener('mouseover', () => {
        clearTimeout(hideTimeout);
      });
    }

    // Mobile: click to toggle
    else {
      buttonEl.addEventListener('click', (e) => {
        if (!panelEl.classList.contains('hidden')) {
          panelEl.classList.add('hidden');
          openPanel = null;
          return;
        }
        e.stopPropagation();
        e.preventDefault();
        panelConfigs.forEach(cfg => {
          const otherPanel = document.querySelector(cfg.panel);
          if (otherPanel && cfg.panel !== panel) otherPanel.classList.add('hidden');
        });
        panelEl.classList.remove('hidden');
        openPanel = panelEl;
        if (panel === '#ratings-filter-panel') {
          initializeRatingsOnVisibility();
        }
      });

      // Hide panel when clicking outside
      document.addEventListener('click', (e) => {
        if (openPanel && !openPanel.contains(e.target) && !buttonEl.contains(e.target)) {
          openPanel.classList.add('hidden');
          openPanel = null;
        }
      });
    }
  });
}

function setupFilterIconHandlers(selector, handler) {
  const el = document.querySelector(selector);
  if (!el) return;
  // Only add touchend handler, NOT click
  el.addEventListener('touchend', (e) => {
    e.preventDefault();
    e.stopPropagation();
    handler === toggleListFilter
      ? touchFilterHandler(e, el)
      : touchWatchedHandler(e, el);
  }, { passive: false });
}

function setupInputChangeHandlers() {
  // Year slider inputs
  const yearRangeMin = document.getElementById('year-range-min');
  const yearRangeMax = document.getElementById('year-range-max');

  if (yearRangeMin) {
    yearRangeMin.addEventListener('input', () => updateYearSliderValues('min'));
  }

  if (yearRangeMax) {
    yearRangeMax.addEventListener('input', () => updateYearSliderValues('max'));
  }

  // Grid size slider
  if (gridSizeSlider) {
    gridSizeSlider.addEventListener('input', () => {
      updateGridSizeValue();
      throttledUpdateGridSizeLayout();
    });

    gridSizeSlider.addEventListener('change', () => {
      saveGridSizePreference(gridSizeSlider.value);
      gridSizeSlider.blur();
    });

    gridSizeSlider.addEventListener('mouseup', () => gridSizeSlider.blur());
    gridSizeSlider.addEventListener('touchend', () => gridSizeSlider.blur());
  }
}

function setupSearchInputHandlers() {
  const searchInput = document.getElementById("search-input");
  if (!searchInput) return;

  let debounceTimeout;

  searchInput.addEventListener("input", () => {
    const query = searchInput.value.trim();
    const addEntryButton = document.getElementById("add-entry");
    if (addEntryButton) {
      addEntryButton.disabled = query.length === 0;
    }

    // Cancel any in-progress search
    if (searchAbortController) {
      searchAbortController.abort();
      searchAbortController = null;
    }

    // Clear existing timeout
    clearTimeout(debounceTimeout);

    // Handle empty query immediately without debounce
    if (!query) {
      activeSearchQuery = null;
      applyFiltersAndSearch(true, null);
      updateMatchCount();
      removeNotification(note => note.isProgress && !note.isBackgroundTask);
      return;
    }

    // For non-empty queries, use debounce
    debounceTimeout = setTimeout(async () => {
      // Create new abort controller for this search
      searchAbortController = new AbortController();
      const signal = searchAbortController.signal;

      try {
        showNotification('Searching...', true);
        await yieldToUI();

        activeSearchQuery = query;
        const selectedCategory = document.getElementById("selected-option").textContent;

        // Perform search based on category
        const results = await performSearch(query, selectedCategory, signal);

        if (signal.aborted) return; // Check if search was aborted

        console.log(`Found ${results.length} results for "${query}" in category "${selectedCategory}".`);

        await yieldToUI();
        applyFiltersAndSearch(true, results);
        updateMatchCount();

      } catch (error) {
        if (error.name === 'AbortError') {
          console.log('Search aborted');
        } else {
          console.error("Search error:", error);
        }
      } finally {
        if (!signal.aborted) {
          removeNotification(note => note.isProgress && !note.isBackgroundTask);
        }
      }
    }, 300);
  });
}

function setupFileInputHandlers() {
    // Restore file input handler
    const restoreFileInput = document.getElementById("restore-file");
    if (restoreFileInput) {
      restoreFileInput.addEventListener("change", async (event) => {
        const files = event.target.files; 
        if (!files || files.length === 0) return;

        try {
          // Convert FileList to Array properly
          const filesArray = Array.from(files);
          console.log(`Selected ${filesArray.length} files. First file: ${filesArray[0]?.name}, Size: ${filesArray[0]?.size}`);

          // Initial notification is shown by the restore function itself
          const result = await restoreDatabase(filesArray, db);

          // Only show success and reload if restore wasn't canceled
          if (result !== false) {
            console.log("Restoration complete.");
            setTimeout(() => location.reload(), 1000);
            showNotification("Restore completed successfully!", false);
          }
        } catch (error) {
          console.error("Error during restore:", error);
          showNotification("Restore failed: " + error.message, false);
          alert("An error occurred during the restore process: " + error.message);
        } finally {
          // Clear the input
          event.target.value = "";
        }
      });
    }

  // Import file input handler
  const importFileInput = document.getElementById("import-file");
  if (importFileInput) {
    importFileInput.addEventListener("change", async (event) => {
      console.log("Import file change event triggered", event.target.files);
      const file = event.target.files[0];
      if (!file) return;

      try {
        // Show loading notification
        showNotification('Processing import file...', true);

        // Read and process the file
        const fileContent = await readFileAsText(file);
        const fileExtension = file.name.split('.').pop().toLowerCase();

        // Process based on file type
        let parsedTitles = [];
        if (fileExtension === 'json') {
          parsedTitles = processJSONFile(fileContent);
        } else if (fileExtension === 'csv') {
          parsedTitles = processCSVFile(fileContent);
        } else if (fileExtension === 'txt') {
          parsedTitles = processTXTFile(fileContent);
        } else {
          throw new Error('Unsupported file format. Please use .txt, .csv, or .json');
        }

        // Show the import dialog with the parsed titles and filename
        showImportDialog(parsedTitles, '', file.name);

        // Show success notification
        showNotification(`Found ${parsedTitles.length} titles`, false);
      } catch (error) {
        console.error('Error processing import file:', error);
        showNotification(`Import error: ${error.message}`, false);
      } finally {
        // Clear the input
        event.target.value = "";
      }
    });
  }
}

function handleRestoreClick(event) {
  event.preventDefault();
  event.stopPropagation();

  // Get the existing file input element
  const restoreFileInput = document.getElementById("restore-file");

  // Clear any existing files
  restoreFileInput.value = "";

  // Trigger the file input click
  restoreFileInput.click();
}

function handleImportClick(event) {
  event.preventDefault();

  // Check if user setup is complete
  if (!userSettings || userSettings.requiresSetup) {
    showNotification("⚠️ Please complete profile setup first before importing titles.", false);
    // Show settings popup with the completeInitialization callback
    showSettingsPopup(completeInitialization);
    return;
  }

  // Get the existing file input element
  const importFileInput = document.getElementById("import-file");

  // Clear any existing files
  importFileInput.value = "";

  // Trigger the file input click
  importFileInput.click();
}

// Populate dropdown with existing custom lists
async function populateListsDropdown() {
  const listSelector = document.getElementById('list-selector');
  if (!listSelector) {
    console.error('List selector element not found');
    return;
  }

  // Start with the "No List" option as default
  listSelector.innerHTML = '<option value="No List" selected>Import without adding to a list</option>';

  try {
    // Open a transaction and get all custom lists
    const transaction = db.transaction(["lists_metadata"], "readonly");
    const store = transaction.objectStore("lists_metadata");

    // Use username_type index to get only current user's custom lists
    const index = store.index("username_type");
    const customLists = await new Promise((resolve, reject) => {
      const request = index.getAll(IDBKeyRange.only([userSettings.username, "custom"]));
      request.onsuccess = () => resolve(request.result);
      request.onerror = (error) => {
        console.error('Error getting custom lists:', error);
        reject(error);
      };
    });

    // Add each list as an option
    if (Array.isArray(customLists)) {
      customLists.forEach(list => {
        const option = document.createElement("option");
        option.value = list.list;
        option.textContent = list.label;
        listSelector.appendChild(option);
      });
    }
    
    // Add the "Add New List..." option at the end
    const newListOption = document.createElement("option");
    newListOption.value = "new";
    newListOption.textContent = "Add New List...";
    listSelector.appendChild(newListOption);
  } catch (error) {
    console.error("Error loading custom lists:", error);
    
    // Add the "Add New List..." option at the end even if there's an error
    const newListOption = document.createElement("option");
    newListOption.value = "new";
    newListOption.textContent = "Add New List...";
    listSelector.appendChild(newListOption);
  }
}

// Create or update list metadata
async function createOrUpdateListMetadata(listName, description) {
  if (!listName || typeof listName !== 'string' || listName.trim() === '') {
    return Promise.reject(new Error('Invalid list name'));
  }

  const transaction = db.transaction(["lists_metadata"], "readwrite");
  const metadataStore = transaction.objectStore("lists_metadata");

  return new Promise((resolve, reject) => {
    // Use the correct key path format: [username, type, list]
    const metadataKey = [userSettings.username, "custom", listName];

    metadataStore.get(metadataKey).onsuccess = (event) => {
      const metadata = event.target.result || {
        username: userSettings.username,
        type: "custom",
        list: listName,
        name: listName,
        created: Date.now(),
        avgRating: 0,
        ratingsCount: 0
      };

      // Update or set description
      metadata.description = description || `Custom list: ${listName}`;
      metadata.lastUpdated = Date.now();

      // Ensure all required fields are present
      if (!metadata.username) metadata.username = userSettings.username;
      if (!metadata.type) metadata.type = "custom";
      if (!metadata.list) metadata.list = listName;
      if (!metadata.avgRating) metadata.avgRating = 0;
      if (!metadata.ratingsCount) metadata.ratingsCount = 0;

      const putRequest = metadataStore.put(metadata);

      putRequest.onsuccess = () => resolve();
      putRequest.onerror = (error) => {
        console.error('Error putting list metadata:', error);
        reject(error);
      };
    };

    metadataStore.get(metadataKey).onerror = (error) => {
      console.error('Error getting list metadata:', error);
      reject(error);
    };

    transaction.onerror = (error) => {
      console.error('Transaction error:', error);
      reject(error);
    };
  });
}

// Function to update the titles section of the dialog
async function updateTitlesInDialog(titles) {
  try {
    // Validate input
    if (!Array.isArray(titles) || titles.length === 0) {
      document.getElementById('titles-section').classList.add('hidden');
      return;
    }

    // Check which titles already exist in the database
    const titlesWithStatus = await Promise.all(
      titles.map(async (item) => {
        if (!item || !item.imdbID) {
          console.error('Invalid title item:', item);
          return null;
        }

        // Check both movies and series stores
        const existingMovie = await getFromDatabase(db, 'movies', item.imdbID);
        const existingSeries = await getFromDatabase(db, 'series', item.imdbID);

        return {
          ...item,
          exists: !!(existingMovie || existingSeries)
        };
      })
    );

    // Filter out null items
    const validTitles = titlesWithStatus.filter(item => item !== null);

    // Sort into new and existing titles
    const newTitles = validTitles.filter(item => !item.exists);
    const existingTitles = validTitles.filter(item => item.exists);

    // Show the titles section
    document.getElementById('titles-section').classList.remove('hidden');

    // Update column headers
    const columnTitle = document.querySelector('.column-title.import');
    if (columnTitle) {
      columnTitle.textContent = `Titles Already in Database (${existingTitles.length})`;
    }

    // Create HTML for the new titles column
    let newTitlesHTML = '';
    newTitles.forEach((item, index) => {
      newTitlesHTML += `
          <label class="title-checkbox" for="title-new-${index}">
            <input type="checkbox" id="title-new-${index}" data-imdbid="${item.imdbID}" checked>
            <span class="title-text">${item.title || 'Unknown Title'} (${item.year || 'N/A'})</span>
          </label>
        `;
    });

    // Create HTML for existing titles column
    let existingTitlesHTML = '';
    existingTitles.forEach((item) => {
      existingTitlesHTML += `
          <div class="title-item">
            ${item.title || 'Unknown Title'} (${item.year || 'N/A'})
          </div>
        `;
    });

    // Update the columns
    const newTitlesColumn = document.getElementById('new-titles-column');
    const existingTitlesColumn = document.getElementById('existing-titles-column');

    if (newTitlesColumn) {
      newTitlesColumn.innerHTML = newTitles.length > 0
        ? newTitlesHTML
        : '<div class="import-placeholder">No new titles found</div>';
    }

    if (existingTitlesColumn) {
      existingTitlesColumn.innerHTML = existingTitles.length > 0
        ? existingTitlesHTML
        : '<div class="import-placeholder">No existing titles found</div>';
    }

    // Set up button event listeners
    setupImportButtons(newTitles.length);
  } catch (error) {
    console.error('Error updating import dialog:', error);
    showNotification('Error loading title information', false);
  }
}

// Setup the buttons and checkbox interactions
function setupImportButtons(newTitlesCount) {
  // Set up toggle button
  const toggleButton = document.getElementById('toggle-selection');
  toggleButton.textContent = 'Deselect All';
  toggleButton.disabled = newTitlesCount === 0;

  // Remove any existing event listeners to prevent duplicates
  const newToggleButton = toggleButton.cloneNode(true);
  toggleButton.parentNode.replaceChild(newToggleButton, toggleButton);

  newToggleButton.addEventListener('click', () => {
    const checkboxes = document.querySelectorAll('.title-checkbox input');
    const allChecked = Array.from(checkboxes).every(cb => cb.checked);

    checkboxes.forEach(cb => {
      cb.checked = !allChecked;
    });

    newToggleButton.textContent = allChecked ? 'Select All' : 'Deselect All';
    updateAddButtonText();
  });

  // Set up add button
  const addButton = document.getElementById('import-add-selected');
  addButton.textContent = newTitlesCount > 0 ? `Add All (${newTitlesCount})` : 'No New Titles';
  addButton.disabled = newTitlesCount === 0;

  // Remove any existing event listeners to prevent duplicates
  const newAddButton = addButton.cloneNode(true);
  addButton.parentNode.replaceChild(newAddButton, addButton);

  newAddButton.addEventListener('click', () => {
    const selectedIDs = Array.from(
      document.querySelectorAll('.title-checkbox input:checked')
    ).map(cb => cb.dataset.imdbid);

    if (selectedIDs.length === 0) {
      showNotification('No titles selected', false);
      return;
    }

    // Get list selection
    const listSelector = document.getElementById('list-selector');
    const selectedOption = listSelector.value;

    // Determine list name and description
    let listName;
    let listDescription = '';

    if (selectedOption === 'new') {
      listName = document.getElementById('list-name').value.trim();
      listDescription = document.getElementById('list-description').value.trim();
    } else {
      // Use existing list
      listName = selectedOption;
    }

    // Close the popup before starting the import process
    closePopup('import-titles');

    // Process the import
    if (listName && listName !== "No List") {
      // Create or update list metadata, then add titles
      createOrUpdateListMetadata(listName, listDescription)
        .then(() => batchAddTitles(selectedIDs, listName))
        .catch(error => {
          console.error('Error creating list metadata:', error);
          // Still try to add titles even if metadata creation fails
          batchAddTitles(selectedIDs, listName);
        });
    } else {
      // Just add titles without a list
      batchAddTitles(selectedIDs, "No List");
    }
  });

  // Add checkbox change listeners
  document.querySelectorAll('.title-checkbox input').forEach(checkbox => {
    // Remove existing listeners to prevent duplicates
    const newCheckbox = checkbox.cloneNode(true);
    checkbox.parentNode.replaceChild(newCheckbox, checkbox);

    newCheckbox.addEventListener('change', () => {
      updateAddButtonText();

      // Update toggle button text
      const allChecked = Array.from(document.querySelectorAll('.title-checkbox input')).every(cb => cb.checked);
      newToggleButton.textContent = allChecked ? 'Deselect All' : 'Select All';
    });
  });

  // Initial update of add button text
  updateAddButtonText();
}

// Helper function to update add button text
function updateAddButtonText() {
  const addButton = document.getElementById('import-add-selected');
  if (!addButton) return;

  const totalCheckboxes = document.querySelectorAll('.title-checkbox input').length;
  const selectedCount = document.querySelectorAll('.title-checkbox input:checked').length;

  if (selectedCount === 0) {
    addButton.textContent = 'No Titles Selected';
    addButton.disabled = true;
  } else if (selectedCount === totalCheckboxes) {
    addButton.textContent = `Add All (${selectedCount})`;
    addButton.disabled = false;
  } else {
    addButton.textContent = `Add Selected (${selectedCount})`;
    addButton.disabled = false;
  }
}

function handlePosterToggle(_event, element) {
  // Find the detail view that contains the poster
  const detailView = element.closest('.detail-view');
  if (!detailView) return;

  const imdbID = detailView.getAttribute("data-imdbid");
  const posterImg = detailView.querySelector("#detail-poster");
  if (!posterImg) return;
  const defaultSrc = posterImg.getAttribute("data-defaultsrc") || "";

  // Revert the custom poster
  clearMediaCustomization(imdbID, "poster");

  // Update the movie cache
  const media = mediaCache.get(imdbID);
  if (media) {
    media.Poster = media.defaultPoster || defaultSrc;
    mediaCache.set(imdbID, media);
  }

  // Update the detail view poster image
  posterImg.src = buildTMDbImageUrl(defaultSrc, "poster");

  // Remove the toggle button
  element.remove();

  // Update the grid view for this movie if present
  const gridCard = document.querySelector(`.media-card[data-imdbid="${imdbID}"]`);
  if (gridCard) {
    const gridPoster = gridCard.querySelector('.media-container img');
    if (gridPoster) {
      gridPoster.src = buildTMDbImageUrl(defaultSrc, "poster");
    }
  }
}

function handlePrevBackdrop(_event, element) {
  if (currentBackdropIndex > 0) {
    currentBackdropIndex--;
    // Retrieve the raw file path from the navigation array
    const rawFilePath = currentBackdrops[currentBackdropIndex];
    // Build the full URL for display
    document.getElementById('imageModalImage').src = buildTMDbImageUrl(rawFilePath, "backdrop");
    document.getElementById('backdrop-counter').textContent =
      `${currentBackdropIndex + 1}/${currentBackdrops.length}`;
    document.getElementById('next-backdrop').style.display = 'block';
    element.style.display = currentBackdropIndex > 0 ? 'block' : 'none';

    // Retrieve the imdbID from the modal data attribute
    const modal = document.getElementById("imageModal");
    const imdbID = modal.dataset.imdbid;
    const media = mediaCache.get(imdbID);
    const defaultFilePath = (media && media.defaultBackdrop) ? media.defaultBackdrop : "";

    // Update the button state using the raw file path
    updateBackdropToggleState(imdbID, rawFilePath, defaultFilePath);
  }
}

function handleNextBackdrop(_event, element) {
  if (currentBackdropIndex < currentBackdrops.length - 1) {
    currentBackdropIndex++;
    const rawFilePath = currentBackdrops[currentBackdropIndex];
    document.getElementById('imageModalImage').src = buildTMDbImageUrl(rawFilePath, "backdrop");
    document.getElementById('backdrop-counter').textContent =
      `${currentBackdropIndex + 1}/${currentBackdrops.length}`;
    document.getElementById('prev-backdrop').style.display = 'block';
    element.style.display =
      currentBackdropIndex < currentBackdrops.length - 1 ? 'block' : 'none';

    const modal = document.getElementById("imageModal");
    const imdbID = modal.dataset.imdbid;
    const media = mediaCache.get(imdbID);
    const defaultFilePath = (media && media.defaultBackdrop) ? media.defaultBackdrop : "";
    updateBackdropToggleState(imdbID, rawFilePath, defaultFilePath);
  }
}

/**
 * Helper function to extract media information from DOM elements
 * @param {HTMLElement} element - The element that was clicked/interacted with
 * @param {string} buttonType - The type of operation (refresh, delete, note, etc.)
 * @returns {Object} Object containing media information
 */
function getMediaInfo(element, buttonType = 'media') {
  // Check if element is a detail view or find the containing media card
  const detailView = element.closest('.detail-view');
  const mediaCard = element.closest('.media-card');
  
  // Determine the container that has our data
  const container = detailView || mediaCard;
  
  if (!container) {
    console.error(`No media container found for ${buttonType} operation:`, element.outerHTML);
    throw new Error("Invalid selection");
  }
  
  // Extract data from the container
  const imdbID = container.dataset.imdbid;
  const mediaType = container.dataset.mediaType || 'movie';
  const title = container.dataset.mediaTitle || '';
  
  // Check for required fields based on operation type
  if (!imdbID) {
    console.error(`Missing imdbID for ${buttonType} operation`);
    throw new Error("Invalid selection");
  }
  
  // Only certain operations require tmdbID
  const tmdbID = container.dataset.tmdbid;
  if (buttonType === 'refresh' && !tmdbID) {
    console.error(`Missing tmdbID for refresh operation`);
    throw new Error("Missing required parameter: tmdbID");
  }
  
  return { 
    imdbID, 
    mediaType, 
    title, 
    tmdbID,
    isDetailView: !!detailView,
    container
  };
}

function initializeRatingsOnVisibility() {
  const panel = document.getElementById("ratings-filter-panel");
  if (panel && !panel.classList.contains("hidden")) {
    const sliders = initializeRatingsFilter();
    if (sliders) {
      setTimeout(() => updateAllRatingValues(sliders), 50);
    }
  }
}

// Helper to determine context
function isFromDetailView() {
  return document.getElementById("popup").classList.contains("detail-view");
}

function updateGridSizeValue() {
  const size = parseInt(gridSizeSlider.value, 10);
  const sliderMin = parseInt(gridSizeSlider.min, 10);
  const sliderMax = parseInt(gridSizeSlider.max, 10);
  const trackWidth = gridSizeSlider.offsetWidth;
  const thumbWidth = 30;

  // Calculate percentage position of the slider value
  const percent = (size - sliderMin) / (sliderMax - sliderMin);
  const valuePosition = `calc(${percent * (trackWidth - thumbWidth)}px + ${thumbWidth / 2}px)`;

  // Update ONLY the thumb value display
  gridSizeValue.textContent = size;
  gridSizeValue.style.left = valuePosition;
}

function updateGridSizeLayout() {
  const size = parseInt(gridSizeSlider.value, 10);
  const sliderMin = parseInt(gridSizeSlider.min, 10);
  const sliderMax = parseInt(gridSizeSlider.max, 10);

  // Calculate normalized percentage (0-1)
  const percent = (size - sliderMin) / (sliderMax - sliderMin);
  const invertedPercent = 1 - percent;

  // Define the grid size state - update the global variable
  const wasAtMaxSize = isAtMaxSize;
  isAtMaxSize = size >= sliderMax;
  const transitionedToMaxSize = !wasAtMaxSize && isAtMaxSize;

  // Apply CSS variables to control the layout
  const cardGrid = document.querySelector('.card-grid');
  if (!cardGrid) return; // Early return if no card grid found
  
  // Set the normalized percentage for CSS calculations
  cardGrid.style.setProperty('--size-percent', percent.toFixed(2));

  // Set card width directly
  cardGrid.style.setProperty('--card-width', `${size}px`);

  // Calculate scale factor (larger scale for smaller cards)
  const scaleFactor = 1.01 + (0.79 * invertedPercent);
  const roundedScale = (Math.round(scaleFactor * 100) / 100).toFixed(2);

  // Set transform scale
  cardGrid.style.setProperty('--card-transform', isAtMaxSize ? 'none' : `scale(${roundedScale})`);

  // Set animation durations
  cardGrid.style.setProperty('--card-hover-duration', `${(0.1 + (0.1 * invertedPercent)).toFixed(2)}s`);
  cardGrid.style.setProperty('--card-unhover-duration', `${(0.3 + (0.3 * invertedPercent)).toFixed(2)}s`);

  // Toggle max-size class
  cardGrid.classList.toggle('max-size', isAtMaxSize);

  // Update all existing images to the new size
  if (filteredRecords && filteredRecords.length > 0 && !isJumping && !jumpLock) {

    // Update image sizes for all cards, with priority for visible ones
    cardGrid.querySelectorAll('.media-card').forEach(mediaCard => {
      const img = mediaCard.querySelector('img');
      if (!img || !img.dataset.originalUrl) return;

      const originalUrl = img.dataset.originalUrl;
      const defaultUrl = img.dataset.defaultUrl || null;
      const isCustom = img.dataset.isCustom === "true";

      // Skip optimization for custom images
      if (isCustom) return;

      try {
        // Direct URL manipulation without imageOptimizer
        if (originalUrl && originalUrl.includes('tmdb.org/t/p/')) {
          // Always use w342 for all grid sizes for consistency and better browser caching
          const optimizedUrl = originalUrl.replace(/\/t\/p\/\w+\//, '/t/p/w342/');
          img.src = optimizedUrl;
        } else {
          // Fallback to default URL if not a TMDB URL
          img.src = defaultUrl || originalUrl;
        }
      } catch (error) {
        console.error("Error optimizing image:", error);
        img.src = defaultUrl || originalUrl;
      }
      
      // If we just transitioned to max size, mark cards for lazy tooltip generation
      if (transitionedToMaxSize && !mediaCard.querySelector('.tooltip-text')) {
        mediaCard.dataset.needsTooltip = "true";
      }
    });
    
    // If we just transitioned to max size, initialize lazy tooltip generation
    if (transitionedToMaxSize) {
      // Proactively create tooltips for visible cards immediately
      const visibleCards = getVisibleCards();
      visibleCards.forEach(card => {
        if (!card.querySelector('.tooltip-text')) {
          card.dataset.needsTooltip = "true";
          createTooltipForCard(card);
        }
      });

      // Initialize lazy tooltips for remaining cards
      initTooltips();
    }
  }

  // Store the old page size before updating
  const oldPageSize = pageSize;
  
  // Update page size immediately (don't use debounced version for this critical update)
  const newPageSize = getPageSize();
  
  // If page size has changed significantly, load more cards if needed
  if (newPageSize > oldPageSize) {
    console.log(`Grid size changed, page size updated from ${oldPageSize} to ${newPageSize}`);
    pageSize = newPageSize;
    
    // Calculate how many more cards we need to load
    const visibleCards = document.querySelectorAll('.card-grid .media-card').length;
    const targetVisibleCards = Math.min(filteredRecords.length, pageSize * 2); // Load 2 pages worth
    
    if (visibleCards < targetVisibleCards) {
      // We need to load more cards
      const additionalNeeded = targetVisibleCards - visibleCards;
      console.log(`Loading ${additionalNeeded} additional cards after size change`);
      
      if (additionalNeeded > 0) {
        // Load more cards at the end
        const nextEnd = Math.min(filteredRecords.length, endIndex + additionalNeeded);
        const additionalRecords = filteredRecords.slice(endIndex, nextEnd);
        
        if (additionalRecords.length > 0) {
          endIndex = nextEnd;
          renderCard(additionalRecords, "append");
          observeMediaCards();
        }
      }
    }
  } else {
    // Still update the page size even if it didn't increase
    pageSize = newPageSize;
  }
  
  // Update observers
  if (isObserving) {
    observeMediaCards();
  }
}

// Helper function to get visible cards
function getVisibleCards() {
  const scrollTop = window.scrollY || window.pageYOffset;
  const viewportHeight = window.innerHeight;
  const viewportBottom = scrollTop + viewportHeight;
  const buffer = viewportHeight * 0.5; // 50% buffer

  return Array.from(document.querySelectorAll('.media-card')).filter(card => {
    const rect = card.getBoundingClientRect();
    const cardTop = rect.top + scrollTop;
    const cardBottom = rect.bottom + scrollTop;

    return (cardTop >= scrollTop - buffer && cardTop <= viewportBottom + buffer) ||
      (cardBottom >= scrollTop - buffer && cardBottom <= viewportBottom + buffer);
  });
}

function handleCardHover(event, card) {
  if (window.isAdjustingTooltipScroll) return;

  const relatedTarget = event.relatedTarget;
  if (relatedTarget && card.contains(relatedTarget)) return;

  const container = card.closest('.card-container');
  if (!container) return;

  window.isAdjustingTooltipScroll = true;

  setTimeout(() => {
    const tooltip = card.querySelector('.card-ttip');
    if (!tooltip) {
      window.isAdjustingTooltipScroll = false;
      return;
    }

    const containerRect = container.getBoundingClientRect();
    const cardRect = card.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();

    let scrollAmount = 0;

    // Check if card extends beyond left edge of container
    if (cardRect.left < containerRect.left) {
      scrollAmount = cardRect.left - containerRect.left - 20; // 20px buffer
    }
    // Check if tooltip extends beyond right edge of container
    else if (tooltipRect.right > containerRect.right) {
      scrollAmount = tooltipRect.right - containerRect.right + 20;
    }

    if (scrollAmount !== 0) {
      container.scrollTo({
        left: container.scrollLeft + scrollAmount,
        behavior: 'smooth'
      });
    }

    setTimeout(() => window.isAdjustingTooltipScroll = false, 300);
  }, 50);
}

// When saving grid size
function saveGridSizePreference(size) {
  if (userSettings && userSettings.username) {
    userSettings.grid_size = parseInt(size, 10);
    saveUserSettingsToDB(userSettings, ['grid_size'])
      .then(success => {
        if (!success) console.error('Failed to save grid size preference');
      })
      .catch(err => console.error('Error saving grid size:', err));
  }
}

function initializeGridSize() {
  // First check if the slider is already at default value
  const currentSliderValue = parseInt(gridSizeSlider.value, 10);
  const sliderMax = parseInt(gridSizeSlider.max, 10);
  isAtMaxSize = currentSliderValue >= sliderMax;

  // Toggle max-size class based on the check - Add these lines
  const cardGrid = document.querySelector('.card-grid');
  if (cardGrid) {
    cardGrid.classList.toggle('max-size', isAtMaxSize);
    console.log(`Grid initialized with${isAtMaxSize ? ' max-size' : ''} class`);
  }

  // Case 1: User has a stored preference
  if (userSettings && userSettings.grid_size !== undefined) {
    const storedSize = parseInt(userSettings.grid_size, 10);

    // Case 1A: Stored value is default, and slider is already at default
    if (storedSize === DEFAULT_CARD_SIZE && currentSliderValue === DEFAULT_CARD_SIZE) {
      // console.log("Grid size is already at default (300). No action needed.");
      pageSize = getPageSize();
      pageSizeCache._cache.set(DEFAULT_CARD_SIZE.toString(), pageSize);
    } else {
      // Case 1B: Stored value needs to be applied
      console.log(`Loading grid size (${storedSize}) from user settings`);
      gridSizeSlider.value = storedSize;
      isAtMaxSize = storedSize >= sliderMax;
      pageSize = getPageSize();
      pageSizeCache._cache.set(storedSize.toString(), pageSize);
      console.log(`Page size set to ${pageSize} based on user settings (${storedSize}px)`);
      
      throttledUpdateGridSizeLayout(); // Update actual grid (debounced for performance)
    }
  }
  // Case 2: No stored preference
  else {
    // Case 2A: Slider is already at default
    if (currentSliderValue === DEFAULT_CARD_SIZE) {
      console.log("Using default grid size (already set to 300)");
      // Don't return early - we still need to position the value display
      pageSize = getPageSize();
      pageSizeCache._cache.set(DEFAULT_CARD_SIZE.toString(), pageSize);
    } else {
      // Case 2B: Need to set to default
      console.log(`Setting default grid size (${DEFAULT_CARD_SIZE})`);
      gridSizeSlider.value = DEFAULT_CARD_SIZE;
      isAtMaxSize = DEFAULT_CARD_SIZE >= sliderMax;
      throttledUpdateGridSizeLayout(); // Update actual grid
    }
  }

  // Always position the value display, regardless of whether the slider value changed
  // Use requestAnimationFrame to ensure DOM is fully rendered before calculating positions
  requestAnimationFrame(() => {
    updateGridSizeValue();
  });
}

// Simplified initTooltips function
function initCardTooltips() {
  // Find all person cards
  const cards = document.querySelectorAll('.card');
  // console.log(`Found ${cards.length} person cards`);

  if (cards.length === 0) return;

  // Add direct event listeners to each card
  cards.forEach(card => {
    // Remove any existing listeners first to prevent duplicates
    card.removeEventListener('mouseenter', cardMouseEnterHandler);

    // Add new listener - using only mouseenter since that's what works
    card.addEventListener('mouseenter', cardMouseEnterHandler);

    // console.log("Added listener to card:", card.querySelector('.card-title')?.textContent);
  });
}

// Define handler functions that can be removed later
function cardMouseEnterHandler(event) {
  handleCardHover(event, this);
}

function handleSeasonCardClick(_event, element) {
  const episodeCount = parseInt(element.getAttribute('data-episodes') || '0');
  if (episodeCount <= 0) return; // Skip seasons with no episodes
  
  const seasonNumber = parseInt(element.getAttribute('data-season'));
  const showID = parseInt(element.getAttribute('data-show-id'));
  const imdbID = element.getAttribute('data-imdb-id');

  loadSeasonEpisodes(showID, seasonNumber, imdbID);

}

// Define the episode card click handler function
function handleEpisodeCardClick(_event, card) {
  const showId = parseInt(card.getAttribute('data-show-id'));
  const seasonNum = parseInt(card.getAttribute('data-season'));
  const episodeNum = parseInt(card.getAttribute('data-episode'));

  // Toggle focused state
  if (card.classList.contains('focused')) {
    // If already focused, remove focus and revert to season filter
    card.classList.remove('focused');
    filterCreditsForSeason(showId, seasonNum);
  } else {
    // Remove focus from other episode cards
    document.querySelectorAll('.card.episode').forEach(c => {
      c.classList.remove('focused');
    });

    // Add focus to this card
    card.classList.add('focused');

    // Apply episode filter
    filterCreditsForEpisode(showId, seasonNum, episodeNum);
  }
}

function toggleWeightedRatingSort() {
  const ratingsFilterButton = document.getElementById('ratings-filter-button');
  const yearFilterButton = document.getElementById('year-filter-button');

  // Reset year sort state
  yearFilterButton.classList.remove('sort-active', 'sort-asc', 'sort-desc');

  // Check if any individual rating sort is active
  const hasActiveRatingSort = currentSortField === 'imdb' ||
    currentSortField === 'tmdb' ||
    currentSortField === 'metascore' ||
    currentSortField === 'rt';

  // Check if weighted rating sort is active
  const isWeightedSortActive = currentSortField === 'weighted';

  // Reset all rating sorts visually
  document.querySelectorAll('.rating-filter, .rating-filter-icon').forEach(element => {
    element.classList.remove('sort-asc', 'sort-desc');
  });

  // Case 1: If any individual rating sort is active, reset to A-Z
  if (hasActiveRatingSort) {
    currentSortField = 'title';
    currentSortOrder = 'asc'; // A-Z is ascending
    ratingsFilterButton.classList.remove('sort-active', 'sort-asc', 'sort-desc');
    showNotification('Sorting by title (A-Z)', false);
  }
  // Case 2: If weighted sort is active, toggle direction or reset
  else if (isWeightedSortActive) {
    if (currentSortOrder === 'desc') {
      // Toggle to ascending
      currentSortOrder = 'asc';
      ratingsFilterButton.classList.remove('sort-desc');
      ratingsFilterButton.classList.add('sort-active', 'sort-asc');
      showNotification('Sorting by weighted rating (ascending)', false);
    } else {
      // Reset to A-Z
      currentSortField = 'title';
      currentSortOrder = 'asc'; // A-Z is ascending
      ratingsFilterButton.classList.remove('sort-active', 'sort-asc', 'sort-desc');
      showNotification('Sorting by title (A-Z)', false);
    }
  }
  // Case 3: No rating sort active, start with weighted descending
  else {
    currentSortField = 'weighted';
    currentSortOrder = 'desc';
    ratingsFilterButton.classList.add('sort-active', 'sort-desc');
    ratingsFilterButton.classList.remove('sort-asc');
    showNotification('Sorting by weighted rating (descending)', false);
  }

  // Apply the sort
  applyFiltersAndSearch(true);
}

// toggle rating sort - maybe reset thumbs too ### 
function toggleRatingSort(ratingType) {
  const ratingsFilterButton = document.getElementById('ratings-filter-button');
  const yearFilterButton = document.getElementById('year-filter-button');

  // Reset year sort state
  yearFilterButton.classList.remove('sort-active', 'sort-asc', 'sort-desc');
  
  // Check if the ratings filter button was clicked
  if (ratingType === 'ratings-filter') {
    // Call the weighted rating sort function
    toggleWeightedRatingSort();
    return;
  } else {
    // Existing logic for individual rating types
    ratingsFilterButton.classList.remove('sort-active', 'sort-asc', 'sort-desc');
    document.querySelectorAll('.rating-filter, .rating-filter-icon').forEach(element => {
      element.classList.remove('sort-asc', 'sort-desc');
    });

    if (currentSortField === ratingType) {
      if (currentSortOrder === 'desc') {
        currentSortOrder = 'asc';
        const section = document.querySelector(`.filter-section:nth-child(${getRatingIconIndex(ratingType)})`);
        section.classList.add('sort-asc');
        section.querySelector('.rating-filter').classList.add('sort-asc');
        section.querySelector('.rating-filter-icon').classList.add('sort-asc');
        ratingsFilterButton.classList.add('sort-active', 'sort-asc');
        showNotification(`Sorting by ${ratingType.toUpperCase()} rating (ascending)`, false);
      } else {
        currentSortField = null;
        currentSortOrder = 'desc'; // Reset to default
        showNotification('Sorting by title (A-Z)', false);
      }
    } else {
      currentSortField = ratingType;
      currentSortOrder = 'desc';
      const section = document.querySelector(`.filter-section:nth-child(${getRatingIconIndex(ratingType)})`);
      section.classList.add('sort-desc');
      section.querySelector('.rating-filter').classList.add('sort-desc');
      section.querySelector('.rating-filter-icon').classList.add('sort-desc');
      ratingsFilterButton.classList.add('sort-active', 'sort-desc');
      showNotification(`Sorting by ${ratingType.toUpperCase()} rating (descending)`, false);
    }
  }

  // Apply the sort
  applyFiltersAndSearch(true);
}

function getRatingIconIndex(ratingType) {
  const indices = { 'imdb': 1, 'tmdb': 2, 'metascore': 3, 'rt': 4 };
  return indices[ratingType] || 1;
}

function showAboutPopup() {
  // Create content for the about popup
  const aboutContent = `
  <div class="about-popup">
    <svg class="about-logo"><use href="#filmy_logo"/></svg>
    <div class="version">Version ${app_version}</div>
    
    <div class="description">
      ${about_content}
      <div class="disclaimer">
      ${app_disclaimer}
      </div>
    </div>
    
    <div class="credits">
      <div class="support">
        <div class="section-title">Support</div>
        <div><a href="https://paypal.me/AlexanderIpfelkofer" target="_blank">paypal.me</a></div>
        <div><a href="https://ko-fi.com/alexanderipfelkofer" target="_blank">buy me a coffee</a></div>
        <div><a href="https://patreon.com/AlexanderIpfelkofer" target="_blank">patreon</a></div>
      </div>
      <div class="subscribe">
      <div class="section-title">Subscribe</div>
      <a href="https://alexanderipfelkofer.substack.com" target="_blank"><svg class="tftd-logo"><use href="#tftd_logo"/></svg></a> 
      </div>
      <div class="powered">
        <div class="section-title">Powered by</div>
        <svg class="tmdb-logo"><use href="#tmdb"/></svg>
        <span class="omdb-logo">+ OMDB</span>
      </div>
    </div>
    
    <div class="copyright">
      ${app_attribution}
      ${app_copyright}
    </div>
  </div>
  `;
  
  // Use the new showPopup function
  showPopup('about', aboutContent);
}

/**
 * Unified function to close any popup/modal
 * @param {string} type - The type of popup to close ('about', 'detail', 'settings', 'video', 'image')
 * @param {Object} options - Additional options for specific popup types
 */

function closePopup(type, options = {}) {
  const overlay = document.getElementById("popup-overlay");
  const popup = document.getElementById("popup");
  const contentContainer = popup?.querySelector(".popup-content");

  // Common actions for most popups
  const commonCleanup = () => {
    if (overlay) overlay.classList.add("hidden");
    if (popup) popup.classList.add("hidden");

    // Only reset styles if this is not a detail view
    if (type !== 'detail') {
      // Reset popup styles
      if (popup) {
        popup.style = '';
        popup.removeAttribute('style');
        popup.className = 'popup hidden';
      }

      // Reset content container styles
      if (contentContainer) {
        contentContainer.style = '';
        contentContainer.removeAttribute('style');

        // Clear content
        contentContainer.innerHTML = '';
        contentContainer.className = 'popup-content';
      }
    }
    document.body.style.overflow = 'auto';
  };

  // Type-specific handling
  switch (type) {
    case 'note':
      commonCleanup();
      break;

    case 'about':
      commonCleanup();
      break;

    case 'import':
    case 'import-confirm':
    case 'selective-add':
      // Handle import-related popups
      commonCleanup();

      // Execute callback if provided
      if (typeof options.onClose === 'function') {
        options.onClose();
      }
      break;

    case 'detail':
      // Remove the "show" class to reverse the fade/scale transition
      if (popup) popup.classList.remove("show");

      // Schedule re-enabling scrolling before the transition fully ends
      setTimeout(() => {
        document.body.style.overflow = "auto";
      }, 50);

      // Wait for the transition end event to hide the popup completely
      popup.addEventListener("transitionend", function handler(e) {
        if (e.propertyName === "transform") {
          if (overlay) overlay.classList.add("hidden");
          if (popup) popup.classList.add("hidden");


          if (contentContainer) contentContainer.innerHTML = "";
          popup.classList.remove("detail-view");
          popup.style.setProperty("--detail-backdrop", "none");

          // Remove direct event listeners from cards
          const cards = document.querySelectorAll('.card');
          cards.forEach(card => {
            card.removeEventListener('mouseenter', cardMouseEnterHandler);
          });

          // Reset tooltip state
          window.isAdjustingTooltipScroll = false;

          // Remove the event listener to avoid duplicate calls
          popup.removeEventListener("transitionend", handler);
        }
      });
      break;

    case 'settings': {
      commonCleanup();

      // Check if JustWatch settings changed - with safe handling of undefined values
      const initialJWConfig = options.initialJWConfig || {};

      // Safe comparison function that handles undefined values
      const safeArrayEquals = (a, b) => {
        // If both are exactly the same (including both undefined), return true
        if (a === b) return true;

        // If either is null/undefined but not both, return false
        if (a == null || b == null) return false;

        // If they don't have lengths, they're not arrays
        if (!Array.isArray(a) || !Array.isArray(b)) return false;

        // Compare lengths
        if (a.length !== b.length) return false;

        // Compare each element
        for (let i = 0; i < a.length; i++) {
          if (a[i] !== b[i]) return false;
        }

        return true;
      };

      // Safely check if JustWatch settings changed
      const jwSettingsChanged =
        initialJWConfig.links !== userSettings.justwatch_links ||
        initialJWConfig.type !== userSettings.justwatch_type ||
        !safeArrayEquals(initialJWConfig.providers, userSettings.justwatch_providers);

      if (jwSettingsChanged) {
        // Clear any existing tooltips
        document.querySelectorAll('.tooltip-text').forEach(tooltip => tooltip.remove());

        // Mark all visible media cards as needing tooltips
        document.querySelectorAll('.media-card').forEach(card => {
          card.dataset.needsTooltip = true;
        });

        // Initialize lazy tooltips to regenerate them
        initTooltips();
      }
      break;
    }
    case 'video': {
      const videoModal = document.getElementById("videoModal");
      const videoFrame = document.getElementById("videoFrame");
      const fromDetailView = options.fromDetailView || false;

      if (videoFrame) videoFrame.src = "";
      if (overlay) overlay.removeEventListener("mousemove", handleVideoMouseMove);

      // Remove event listener for showing close button
      if (overlay) overlay.removeEventListener("mousemove", showCloseButton);

      // Hide modal
      if (videoModal) {
        videoModal.classList.remove("active");
        videoModal.classList.add("hidden");
        videoModal.classList.remove("external-view");
      }
      if (popup) popup.classList.remove("external-view");

      // Context-aware container handling
      if (!fromDetailView) {
        if (overlay) overlay.classList.add("hidden");
        if (popup) popup.classList.add("hidden");
      } else {
        if (overlay) overlay.classList.remove("hidden");
        if (popup) popup.classList.remove("hidden");
      }
      break;
    }
    case 'image': {
      const modal = document.getElementById("imageModal");
      if (modal) {
        modal.classList.add("hidden");
        removeModalKeyListeners();
        modal.removeEventListener('mousemove', resetTimer);
        modal.removeEventListener('mouseenter', resetTimer);
        modal.removeEventListener('mousemove', stopSlideshow);
        modal.removeEventListener('click', handleMouseActivity);
        clearTimeout(mouseTimer);
        clearTimeout(fadeTimeout);
        stopSlideshow();
        if (slideshowInterval) {
          clearInterval(slideshowInterval);
          slideshowInterval = null;
        }
        // Remove any lingering dynamic overlay
        const existingOverlay = document.querySelector(".dynamic-overlay");
        if (existingOverlay) {
          existingOverlay.parentNode.removeChild(existingOverlay);
        }
        // Reset cursor visibility
        modal.classList.remove("hide-cursor");
      }
      break;
    }
    default:
      // Generic popup closing
      commonCleanup();
  }
}

function handleMediaCardClick(event, element) {
  // Check if the click was on a rating link or tooltip
  if (event.target.closest('.rating-link') || event.target.closest('.ttip-txt')) {
    // Let the link handle this
    return;
  }

  // Check if the click was on the detail panel or its children
  if (event.target.closest('.detail-panel')) {
    // Let the detail panel handler handle this
    return;
  }

  // Open detail view for all card sizes
  handleDetailView(event, element);
}

function handleResizedGridCardHover(_event, card) {
  const topMenuHeight = 52;
  const scrollPadding = 24;

  // Get reference to the card-grid element
  const cardGrid = document.querySelector('.card-grid');
  if (!cardGrid) return;

  // Skip if at max size - now checking from card-grid class
  if (cardGrid.classList.contains('max-size')) return;

  // Get the current scale factor from CSS variable
  const scaleStr = getComputedStyle(cardGrid)
    .getPropertyValue('--card-transform')
    .match(/scale\(([^)]+)\)/);

  if (!scaleStr || !scaleStr[1]) return;

  const scale = parseFloat(scaleStr[1]);
  if (isNaN(scale) || scale <= 1) return;

  // Get current card dimensions and position
  const rect = card.getBoundingClientRect();
  const scaledWidth = rect.width * scale;
  const scaledHeight = rect.height * scale;
  const expandX = (scaledWidth - rect.width) / 2;
  const expandY = (scaledHeight - rect.height) / 2;

  const scaledLeft = rect.left - expandX;
  const scaledTop = rect.top - expandY;
  const scaledRight = rect.right + expandX;
  const scaledBottom = rect.bottom + expandY;

  const outsideLeft = scaledLeft < 0;
  const outsideTop = scaledTop < topMenuHeight;
  const outsideRight = scaledRight > window.innerWidth;
  const outsideBottom = scaledBottom > window.innerHeight;

  setTimeout(() => {
    if (outsideLeft) {
      card.style.transformOrigin = 'left center';
    } else if (outsideRight) {
      card.style.transformOrigin = 'right center';
    } else {
      card.style.transformOrigin = 'center center';
    }

    let scrollY = 0;
    if (outsideTop) {
      scrollY = scaledTop - topMenuHeight - scrollPadding;
    } else if (outsideBottom) {
      scrollY = scaledBottom - window.innerHeight + scrollPadding;
    }

    if (scrollY !== 0) {
      const currentScrollY = window.scrollY;
      const newScrollY = currentScrollY + scrollY;
      window.scrollTo({
        top: newScrollY,
        behavior: 'smooth'
      });
    }
  }, 50);
}

// Helper function to read file as text
function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => resolve(e.target.result);
    reader.onerror = () => reject(new Error('Error reading file'));
    reader.readAsText(file);
  });
}

function processCSVFile(content) {
  const lines = content.split('\n');

  // Detect delimiter (tab or comma)
  const firstLine = lines[0];
  const delimiter = firstLine.includes('\t') ? '\t' : ',';

  const headers = firstLine.split(delimiter).map(h => h.trim().replace(/"/g, ''));

  // Find the index of the relevant columns
  const constIndex = headers.findIndex(h => h === 'Const');
  const titleIndex = headers.findIndex(h => h === 'Title');
  const yearIndex = headers.findIndex(h => h === 'Year');

  if (constIndex === -1) {
    throw new Error('Invalid CSV format: "Const" column not found');
  }

  // Extract IMDb IDs, titles, and years
  const titles = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;

    // Handle CSV properly (account for commas within quoted fields)
    const values = parseCSVLine(lines[i]);

    if (values.length > constIndex) {
      const imdbID = values[constIndex].replace(/"/g, '').trim();

      // Get title and year if available
      const title = titleIndex > -1 && values.length > titleIndex ?
        values[titleIndex].replace(/"/g, '').trim() : 'Unknown';
      const year = yearIndex > -1 && values.length > yearIndex ?
        values[yearIndex].replace(/"/g, '').trim() : '';

      if (imdbID && imdbID.startsWith('tt')) {
        titles.push({ imdbID, title, year });
      }
    }
  }

  return titles;
}

// Parse a CSV line properly (handling quoted fields)
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  result.push(current);
  return result;
}

// Process text file (one IMDb ID per line)
function processTXTFile(content) {
  const lines = content.split('\n');
  return lines
    .map(line => line.trim())
    .filter(line => line && line.startsWith('tt'));
}

// Process JSON file
function processJSONFile(content) {
  try {
    const data = JSON.parse(content);

    // Handle different JSON formats
    if (Array.isArray(data)) {
      // If it's an array, look for IMDb IDs
      return data
        .filter(item => typeof item === 'string' && item.startsWith('tt'))
        .map(id => id);
    } else if (data.items && Array.isArray(data.items)) {
      // If it has an 'items' property that's an array
      return data.items
        .filter(item => item.imdbID && item.imdbID.startsWith('tt'))
        .map(item => item.imdbID);
    } else {
      // Try to extract IMDb IDs from any object properties
      const ids = [];
      const extractIDs = (obj) => {
        for (const key in obj) {
          if (typeof obj[key] === 'string' && obj[key].startsWith('tt')) {
            ids.push(obj[key]);
          } else if (typeof obj[key] === 'object' && obj[key] !== null) {
            extractIDs(obj[key]);
          }
        }
      };

      extractIDs(data);
      return ids;
    }
  } catch {
    throw new Error('Invalid JSON format');
  }
}

// Main function to show and manage the import dialog
function showImportDialog(titles = null, listName = '', fileName = '') {
  // Create the basic structure first
  const popupContent = `
      <div class="import-dialog">
        <h2>Import Titles</h2>
        
        <!-- File selection and list information section -->
        <div class="import-form">
          <div class="titles-header import">
            <div class="left-section import">
              <button id="select-import-file" class="popup-button">
                <svg class="icon-svg"><use href="#file_txt" /></svg>
                <span class="button-text">Select File</span>
              </button>
              <span id="file-name-display" class="default-filename">${fileName}</span>
            </div>
            
            <div class="right-section import">
              <select id="list-selector" class="popup-input">
                <option value="No List" selected>Import without adding to a list</option>
                <!-- Existing lists will be populated here -->
                <option value="new">Add New List...</option>
              </select>
            </div>
          </div>
  
          <!-- List name and description fields that show/hide based on selection -->
          <div id="new-list-fields" class="hidden">
            <div class="form-row">
              <label for="list-name">New List Name (max 100 characters)</label>
              <input type="text" id="list-name" maxlength="100" class="popup-input" 
                    placeholder="My Watchlist" value="${listName}" />
            </div>
            
            <div class="form-row">
              <textarea id="list-description" maxlength="500" class="popup-input" 
                        placeholder="About this list... (optional)"></textarea>
            </div>
          </div>
        </div>
        
        <!-- Titles section (initially hidden) -->
        <div id="titles-section" class="hidden">
          <div class="titles-header import">
            <div class="left-section import">
              <button class="popup-button" id="toggle-selection">Deselect All</button>
              <button id="import-add-selected" class="popup-button">Add Selected (0)</button>
            </div>
            <div class="right-section import">
              <span class="column-title import">Titles Already in Database (0)</span>
            </div>
          </div>
          
          <div class="titles-container import">
            <div class="titles-column import" id="new-titles-column">
              <!-- New titles will be populated here -->
              <div class="import-placeholder">Select a file to import titles</div>
            </div>
            <div class="titles-column import" id="existing-titles-column">
              <!-- Existing titles will be populated here -->
            </div>
          </div>
        </div>
      </div>
    `;

  // Show popup first, then populate the list
  showPopup('import-titles', popupContent);

  // Ensure the DOM is ready before adding event listeners
  setTimeout(() => {
    populateListsDropdown();

    // Toggle new list fields based on dropdown selection
    const listSelector = document.getElementById('list-selector');
    if (listSelector) {
      listSelector.addEventListener('change', function () {
        const newListFields = document.getElementById('new-list-fields');
        if (newListFields) {
          if (this.value === 'new') {
            newListFields.classList.remove('hidden');
          } else {
            newListFields.classList.add('hidden');
          }
        }
      });
    }

    // Add event listener for list name input to update dropdown when a new list is created
    const listNameInput = document.getElementById('list-name');
    if (listNameInput) {
      listNameInput.addEventListener('blur', function () {
        const newListName = this.value.trim();
        if (newListName) {
          // Check if this list name already exists in the dropdown
          const listSelector = document.getElementById('list-selector');
          let exists = false;

          for (let i = 0; i < listSelector.options.length; i++) {
            if (listSelector.options[i].text === newListName) {
              exists = true;
              listSelector.selectedIndex = i;
              break;
            }
          }

          // If it doesn't exist, add it to the dropdown
          if (!exists) {
            const newOption = new Option(newListName, newListName);
            // Insert before the "Add New List..." option (assuming it's the last one)
            listSelector.add(newOption, listSelector.options.length - 1);
            listSelector.value = newListName;
          }
        }
      });
    }

    // File selection handler
    const selectFileButton = document.getElementById('select-import-file');
    if (selectFileButton) {
      selectFileButton.addEventListener('click', () => {
        const fileInput = document.createElement('input');
        fileInput.type = 'file';
        fileInput.accept = '.csv,.txt,.json';
        fileInput.style.display = 'none';

        fileInput.onchange = async (e) => {
          const file = e.target.files[0];
          if (file) {
            try {
              // Display the file name
              const fileNameDisplay = document.getElementById('file-name-display');
              if (fileNameDisplay) {
                fileNameDisplay.textContent = file.name;
              }

              // Show loading notification
              showNotification('Processing import file...', true);

              // Read and process the file
              const fileContent = await readFileAsText(file);
              const fileExtension = file.name.split('.').pop().toLowerCase();

              // Process based on file type
              let parsedTitles = [];
              if (fileExtension === 'json') {
                parsedTitles = processJSONFile(fileContent);
              } else if (fileExtension === 'csv') {
                parsedTitles = processCSVFile(fileContent);
              } else if (fileExtension === 'txt') {
                parsedTitles = processTXTFile(fileContent);
              } else {
                throw new Error('Unsupported file format. Please use .txt, .csv, or .json');
              }

              // Update the dialog with parsed titles
              await updateTitlesInDialog(parsedTitles);

              // Show success notification
              showNotification(`Found ${parsedTitles.length} titles`, false);
            } catch (error) {
              console.error('Error processing import file:', error);
              showNotification(`Import error: ${error.message}`, false);
            }
          }
        };

        document.body.appendChild(fileInput);
        fileInput.click();
        document.body.removeChild(fileInput);
      });
    }

    // If titles are already provided, update the dialog immediately
    if (titles && titles.length > 0) {
      updateTitlesInDialog(titles);
    }
  }, 0);
}

// Batch add titles function
async function batchAddTitles(imdbIDs, listName) {
  if (!Array.isArray(imdbIDs) || imdbIDs.length === 0) {
    showNotification('No titles to add', false);
    return;
  }

  showNotification(`Adding ${imdbIDs.length} titles to your database...`, true);

  // --- Get or create the custom list and get its UUID ---
  let targetList = null;
  let listId = null;
  let description = '';
  if (listName && listName !== "No List") {
    // Try to find the list by label (case-insensitive)
    const userLists = await getUserLists(userSettings.username, 'custom');
    targetList = userLists.find(l => l.label.trim().toLowerCase() === listName.trim().toLowerCase());

    // Get description from input if available
    const descriptionField = document.getElementById('list-description');
    description = descriptionField ? descriptionField.value.trim() : '';

    // If not found, create it
    if (!targetList) {
      try {
        targetList = await createUserList(userSettings.username, listName, description, false);
        await reloadListMetadata(userSettings.username);
        await loadListStates();
        refreshCustomListsInTooltips();  
      } catch (err) {
        showNotification('Error creating custom list: ' + err.message, false);
        // Still proceed, but skip adding to list
      }
    }
    listId = targetList ? targetList.list : null;
  }

  // --- Add each title to the list using the UUID (listId) ---
  if (listId) {
    for (const imdbID of imdbIDs) {
      if (!imdbID) continue;
      await saveToDatabase(db, {
        username: userSettings.username,
        imdbID: imdbID,
        type: 'custom',
        list: listId,
        value: true,
        dateAdded: Date.now()
      }, 'lists');
    }
  }

  // --- Process titles in batches as before ---
  const batchSize = 5;
  let processedCount = 0, successCount = 0, errorCount = 0;
  let successfulImdbIDs = [];
  let mediaTypes = {};

  for (let i = 0; i < imdbIDs.length; i += batchSize) {
    const batch = imdbIDs.slice(i, i + batchSize);
    await Promise.all(
      batch.map(async (imdbID) => {
        if (!imdbID) {
          processedCount++; errorCount++; return;
        }
        try {
          const existingMovie = await getFromDatabase(db, 'movies', imdbID);
          if (existingMovie) {
            processedCount++; successCount++; successfulImdbIDs.push(imdbID); mediaTypes[imdbID] = 'movie'; return;
          }
          const existingSeries = await getFromDatabase(db, 'series', imdbID);
          if (existingSeries) {
            processedCount++; successCount++; successfulImdbIDs.push(imdbID); mediaTypes[imdbID] = 'tv'; return;
          }
          const result = await addTitle(db, { imdb_ID: imdbID, category: 'all' });
          if (result) {
            successCount++; successfulImdbIDs.push(imdbID);
            const movie = await getFromDatabase(db, 'movies', imdbID);
            const series = await getFromDatabase(db, 'series', imdbID);
            mediaTypes[imdbID] = movie ? 'movie' : (series ? 'tv' : null);
          } else {
            errorCount++;
          }
        } catch (error) {
          console.error(`Error adding title ${imdbID}:`, error);
          errorCount++;
        } finally {
          processedCount++;
          if (processedCount < imdbIDs.length) {
            showNotification(`Processing: ${processedCount}/${imdbIDs.length} titles...`, true);
          }
        }
      })
    );
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  showNotification(`Import complete: ${successCount} titles added, ${errorCount} errors`, false);

  // Update cache for each successfully added title
  for (const imdbID of successfulImdbIDs) {
    const mediaType = mediaTypes[imdbID];
    if (mediaType) {
      await updateCache(imdbID, db, mediaType);

      const existingCard = document.querySelector(`.media-card[data-imdbid="${imdbID}"]`);
      if (!existingCard) {
        const updatedRecord = mediaCache.get(imdbID);
        if (updatedRecord) {
          convertYearToNumber(updatedRecord);
          filteredRecords.push(updatedRecord);
          totalRecords += 1;
          renderCard([updatedRecord], "prepend");
        }
      } else {
        await updateMediaCard(imdbID, mediaType);
      }
    }
  }

  updateActiveYears();
  initializeGenreFilter();
  initializeYearSlider();
  updateMatchCount();

  applyFiltersAndSearch(true);
}

/**
 * Shows a popup with the specified content
 * @param {string} type - Type of popup (e.g., 'about', 'import', 'settings')
 * @param {string|HTMLElement} content - HTML content or DOM element to show in the popup
 * @param {Object} options - Additional options for the popup
 */
function showPopup(type, content, options = {}) {
  const popup = document.getElementById("popup");
  const overlay = document.getElementById("popup-overlay");
  const contentContainer = popup.querySelector(".popup-content");

  // Clear any existing classes and add the type-specific class
  popup.className = "popup";
  popup.classList.add(`${type}-popup`);

  // Set data attribute for type (useful for event handling)
  popup.setAttribute('data-popup-type', type);

  // Prevent scrolling of the body
  document.body.style.overflow = 'hidden';

  // Add close button if not explicitly disabled
  let closeButton = '';
  if (options.hideCloseButton !== true) {
    closeButton = `<button class="close-modal" data-popup-type="${type}">×</button>`;
  }

  // Set content
  if (typeof content === 'string') {
    contentContainer.innerHTML = closeButton + content;
  } else if (content instanceof HTMLElement) {
    contentContainer.innerHTML = closeButton;
    contentContainer.appendChild(content);
  }

  // Apply any custom styles to the popup
  if (options.styles) {
    Object.assign(popup.style, options.styles);
  }

  // Apply any custom styles to the content container
  if (options.contentStyles) {
    Object.assign(contentContainer.style, options.contentStyles);
  }

  // Show overlay and popup
  overlay.classList.remove("hidden");
  popup.classList.remove("hidden");

  // Add any custom event listeners
  if (options.listeners) {
    for (const [selector, events] of Object.entries(options.listeners)) {
      const elements = contentContainer.querySelectorAll(selector);
      elements.forEach(element => {
        for (const [event, handler] of Object.entries(events)) {
          element.addEventListener(event, handler);
        }
      });
    }
  }

  // Return the popup element for further manipulation
  return popup;
}

/**
 * Show Note Editor Popup
 * @param {string} imdbID - The IMDb ID
 * @param {string} title - Movie/Series title
 */
async function showNoteEditorPopup(imdbID, title) {

  // Get existing note
  const note = await getNote(imdbID);

  // Create note editor content
  const content = document.createElement('div');
  content.className = 'note-editor-container';

  // Header with title
  const header = document.createElement('h3');
  header.textContent = `Notes for "${title}"`;
  content.appendChild(header);

  // Format toolbar
  const toolbar = document.createElement('div');
  toolbar.className = 'format-toolbar';
  toolbar.innerHTML = `
    <button type="button" class="format-button" data-format="bold" title="Bold">B</button>
    <button type="button" class="format-button" data-format="italic" title="Italic">I</button>
    <button type="button" class="format-button" data-format="strike" title="Strikethrough">S</button>
    <button type="button" class="format-button" data-format="h2" title="Heading">H</button>
  `;
  content.appendChild(toolbar);

  // Editor
  const editor = document.createElement('textarea');
  editor.className = 'note-editor';
  editor.placeholder = 'Add your notes here...';
  editor.value = note ? note.content : '';
  content.appendChild(editor);

  // Preview
  const preview = document.createElement('div');
  preview.className = 'note-preview';
  preview.style.display = 'none';
  preview.innerHTML = parseMarkdown(editor.value);
  content.appendChild(preview);

  // Buttons
  const buttons = document.createElement('div');
  buttons.className = 'note-buttons';
  buttons.innerHTML = `
    <button type="button" class="save-note">Save</button>
    <button type="button" class="toggle-preview">Preview</button>
    <button type="button" class="delete-note" ${!note ? 'disabled' : ''}>Delete</button>
  `;
  content.appendChild(buttons);

  // Show popup without inline styles
  showPopup('note', content, {
    listeners: {
      '.format-button': {
        click: (e) => {
          const format = e.target.getAttribute('data-format');
          const textarea = editor;
          const start = textarea.selectionStart;
          const end = textarea.selectionEnd;
          const selectedText = textarea.value.substring(start, end);
          let replacement = '';

          switch (format) {
            case 'bold':
              replacement = `**${selectedText}**`;
              break;
            case 'italic':
              replacement = `_${selectedText}_`;
              break;
            case 'strike':
              replacement = `~~${selectedText}~~`;
              break;
            case 'h2':
              replacement = `## ${selectedText}`;
              break;
          }

          if (selectedText) {
            textarea.value =
              textarea.value.substring(0, start) +
              replacement +
              textarea.value.substring(end);
            textarea.selectionStart = start;
            textarea.selectionEnd = start + replacement.length;
          }

          textarea.focus();
        }
      },
      '.save-note': {
        click: async () => {
          await saveNote(imdbID, editor.value);
          showNotification('Note saved', false);
          closePopup('note');

          // Update the note icon in the UI if needed
          updateNoteIconStatus(imdbID, editor.value.trim().length > 0);
        }
      },
      '.toggle-preview': {
        click: () => {
          const isPreviewVisible = preview.style.display === 'block';

          if (isPreviewVisible) {
            preview.style.display = 'none';
            editor.style.display = 'block';
            document.querySelector('.toggle-preview').textContent = 'Preview';
          } else {
            preview.innerHTML = parseMarkdown(editor.value);
            preview.style.display = 'block';
            editor.style.display = 'none';
            document.querySelector('.toggle-preview').textContent = 'Edit';
          }
        }
      },
      '.delete-note': {
        click: async () => {
          const confirmed = await showCustomConfirm('Are you sure you want to delete this note?');
          if (confirmed) {
            await deleteNote(imdbID);
            showNotification('Note deleted', false);
            closePopup('note');

            // Update the note icon in the UI
            updateNoteIconStatus(imdbID, false);
          }
        }
      }
    }
  });
}

/**
 * Get note for a movie/series
 * @param {string} imdbID - The IMDb ID
 * @returns {Promise<Object|null>} - Note object or null if not found
 */
async function getNote(imdbID) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(['notes'], 'readonly');
    const store = transaction.objectStore('notes');
    const index = store.index('username_imdbID');
    const request = index.get([userSettings.username , imdbID]);

    request.onsuccess = () => {
      resolve(request.result || null);
    };

    request.onerror = () => {
      console.error(`Error retrieving note for ${imdbID}:`, request.error);
      reject(request.error);
    };
  });
}

/**
 * Add or update note for a movie/series
 * @param {string} imdbID - The IMDb ID
 * @param {string} content - Note content
 * @returns {Promise<number>} - Note ID
 */
async function saveNote(imdbID, content) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(['notes'], 'readwrite');
    const store = transaction.objectStore('notes');
    const index = store.index('username_imdbID');

    // Check if note already exists
    const getRequest = index.get([userSettings.username , imdbID]);

    getRequest.onsuccess = () => {
      const now = Date.now();

      if (getRequest.result) {
        // Update existing note
        const note = getRequest.result;
        note.content = content;
        note.modified = now;

        const updateRequest = store.put(note);
        updateRequest.onsuccess = () => {
          resolve(note.id);
        };

        updateRequest.onerror = () => {
          console.error(`Error updating note for ${imdbID}:`, updateRequest.error);
          reject(updateRequest.error);
        };
      } else {
        // Create new note
        const note = {
          imdbID: imdbID,
          username: userSettings.username ,
          content: content,
          timestamp: now,
          modified: now
        };

        const addRequest = store.add(note);

        addRequest.onsuccess = () => {
          resolve(addRequest.result);
        };

        addRequest.onerror = () => {
          console.error(`Error adding note for ${imdbID}:`, addRequest.error);
          reject(addRequest.error);
        };
      }
    };

    getRequest.onerror = () => {
      console.error(`Error checking for existing note for ${imdbID}:`, getRequest.error);
      reject(getRequest.error);
    };
  });
}

/**
 * Delete note for a movie/series
 * @param {string} imdbID - The IMDb ID
 * @returns {Promise<boolean>} - Success or failure
 */
async function deleteNote(imdbID) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(['notes'], 'readwrite');
    const store = transaction.objectStore('notes');
    const index = store.index('username_imdbID');

    // Find the note
    const getRequest = index.get([userSettings.username , imdbID]);

    getRequest.onsuccess = () => {
      if (getRequest.result) {
        // Delete the note
        const deleteRequest = store.delete(getRequest.result.id);

        deleteRequest.onsuccess = () => {
          resolve(true);
        };

        deleteRequest.onerror = () => {
          console.error(`Error deleting note for ${imdbID}:`, deleteRequest.error);
          reject(deleteRequest.error);
        };
      } else {
        // No note found
        resolve(false);
      }
    };

    getRequest.onerror = () => {
      console.error(`Error finding note for ${imdbID}:`, getRequest.error);
      reject(getRequest.error);
    };
  });
}

/**
 * Handle note media button click
 * @param {Event} event - The click event
 * @param {HTMLElement} element - The clicked element
 */
async function handleNoteMedia(event, element) {
  event.preventDefault();
  event.stopPropagation();

  try {
    const media = getMediaInfo(element, 'note');

    if (media) {
      showNoteEditorPopup(media.imdbID, media.title);
    }
  } catch (error) {
    console.error('Error handling note media:', error);
    showNotification('⚠️ Could not add note: ' + error.message, false);
  }
}

/**
 * Update note icon status in the UI
 * @param {string} imdbID - The IMDb ID
 * @param {boolean} hasNote - Whether the movie/series has a note
 */
function updateNoteIconStatus(imdbID, hasNote) {
  // Find all note icons for this IMDb ID
  const noteIcons = document.querySelectorAll(`.note-media[data-imdb-id="${imdbID}"]`);

  noteIcons.forEach(icon => {
    if (hasNote) {
      icon.classList.add('has-note');
      icon.setAttribute('title', 'Edit note');
    } else {
      icon.classList.remove('has-note');
      icon.setAttribute('title', 'Add note');
    }
  });
}

/**
 * Simple Markdown Parser
 * @param {string} text - Markdown text
 * @returns {string} - HTML output
 */
function parseMarkdown(text) {
  if (!text) return '';

  // Escape HTML
  let html = text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  // Bold: **text** or __text__
  html = html.replace(/(\*\*|__)(.*?)\1/g, '<strong>$2</strong>');

  // Italic: *text* or _text_
  html = html.replace(/(\*|_)(.*?)\1/g, '<em>$2</em>');

  // Strikethrough: ~~text~~
  html = html.replace(/~~(.*?)~~/g, '<s>$1</s>');

  // Headings: # Heading
  html = html.replace(/^# (.*$)/gm, '<h1>$1</h1>');
  html = html.replace(/^## (.*$)/gm, '<h2>$1</h2>');
  html = html.replace(/^### (.*$)/gm, '<h3>$1</h3>');

  // Links: [title](url)
  html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');

  // Line breaks
  html = html.replace(/\n/g, '<br>');

  return html;
}

function displayInitializationError(error) {
  // Implement UI to show a critical error message
  const body = document.body;
  body.innerHTML = `<div style="padding: 20px; text-align: center; color: red;">
        <h1>Application Error</h1>
        <p>Failed to initialize the application. Please try refreshing the page.</p>
        <p><small>${error.message}</small></p>
        </div>`;
}

/**
 * Loads critical data (user settings, list configs) after DB is open.
 * Determines if initial user setup is required.
 * @param {IDBDatabase} dbConn - The opened DB connection.
 * @returns {Promise<{setupNeeded: boolean}>} - Status indicating if initial setup is required.
 */
async function loadCoreAppData(dbConn) {
  // 1. Try to get username hint from localStorage
  const currentUsername = localStorage.getItem('lastUsername') || null;

  // console.log(`Attempting to load core app data for user hint: ${currentUsername}`);

  let settingsResult = null;
  let allUsernames = [];
  let setupNeeded = false;

  try {
    // --- Fetch Usernames and Hinted User Settings Concurrently ---
    const transaction = dbConn.transaction(["user_settings"], "readonly");
    const store = transaction.objectStore("user_settings");

    const promises = [];

    // Promise 1: Get all usernames (keys)
    promises.push(new Promise((resolve, reject) => {
      const req = store.getAllKeys();
      req.onsuccess = event => resolve(event.target.result || []);
      req.onerror = event => {
        console.error("Failed to get all usernames:", event.target.error);
        reject(new Error(`Failed to get all usernames: ${event.target.error?.message}`));
      };
    }));

    // Promise 2: Get settings for hinted user (or resolve null)
    if (currentUsername) {
      promises.push(new Promise((resolve) => {
        const req = store.get(currentUsername);
        req.onsuccess = event => resolve(event.target.result || null);
        req.onerror = event => {
          console.warn(`Failed to load settings for hinted user ${currentUsername}: ${event.target.error}`);
          resolve(null);
        };
      }));
    } else {
      promises.push(Promise.resolve(null));
    }

    // Wait for username list and hinted settings results
    const [keysResult, hintedSettingsResult] = await Promise.all(promises);

    allUsernames = keysResult;
    settingsResult = hintedSettingsResult;

    // console.log(`DB checks complete. Usernames found: ${allUsernames.length}, Hinted settings loaded: ${!!settingsResult}`);

    // --- Determine Application State (Setup Needed?) ---
    if (settingsResult) {
      // Settings loaded successfully for the hinted user
      userSettings = settingsResult; // Assign to global state
      localStorage.setItem('lastUsername', userSettings.username); // Refresh hint
      console.log(`User settings loaded for: ${userSettings.username}`);
      setupNeeded = false;
    } else {
      // Settings for hinted user failed to load, or no hint was given
      if (allUsernames.length === 0) {
        // ---> Scenario: No users exist in the database <---
        console.log("No users found in DB. Initial setup required.");
        userSettings = { requiresSetup: true }; // Set flag in global state
        localStorage.removeItem('lastUsername'); // Clear any invalid hint
        setupNeeded = true; // Signal to caller
      } else {
        // ---> Scenario: Users exist, but hint failed or was missing <---
          if (currentUsername) {
            console.info(`User '${currentUsername}' from localStorage not found. Falling back to user: ${allUsernames[0]}`);
          } else {
            console.info(`No user hint in localStorage. Using existing user: ${allUsernames[0]}`);
          }
        // Fallback: Load the first user found
        const fallbackUsername = allUsernames[0];
        localStorage.setItem('lastUsername', fallbackUsername); // Set hint to fallback user
        console.log(`Falling back to first user: ${fallbackUsername}`);
        try {
          // Attempt to load settings for the fallback user
          settingsResult = await getUserSettingsFromDB(fallbackUsername);
          if (!settingsResult) {
            throw new Error("Empty settings returned");
          }
          userSettings = settingsResult;
          console.log("Loaded settings for fallback user.");
        } catch (fallbackError) {
          console.error(`Failed to load settings for fallback user ${fallbackUsername}:`, fallbackError);
          // Create minimal valid settings instead of just username
          userSettings = {
            username: fallbackUsername,
            requiresSetup: false,
            omdb_apikeys: [],
            justwatch_providers: []
          };
        }
        // Crucially, setup is NOT needed if other users exist
        setupNeeded = false;
      }
    }

    // Ensure appSettings exists, even if empty (except for setup flag case)
    if (!setupNeeded && (!userSettings || Object.keys(userSettings).length === 0)) {
      console.warn("appSettings appears empty after load/fallback, ensuring defaults.");
      userSettings = { username: currentUsername || allUsernames[0] };
    }

    // --- Load List Configurations ---
    try {
      await new Promise((resolve, reject) => {
        const transactionLists = dbConn.transaction(["lists_metadata"], "readonly");
        const listsMetadataStore = transactionLists.objectStore("lists_metadata");
        const listsUsernameIndex = listsMetadataStore.index('username');

        const listPromises = [];

        // Promise 1: Get default lists
        listPromises.push(new Promise((res, rej) => {
          const req = listsUsernameIndex.getAll(IDBKeyRange.only('default'));
          req.onsuccess = e => res(e.target.result || []);
          req.onerror = e => {
            console.error("Failed to load default lists:", e.target.error);
            rej(new Error(`Failed to load default lists: ${e.target.error?.message}`));
          };
        }));

        // Promise 2: Get user-specific lists (only if a user is set and setup isn't needed)
        if (userSettings.username && userSettings.username !== 'default' && !setupNeeded) {
          listPromises.push(new Promise((res, rej) => {
            const req = listsUsernameIndex.getAll(IDBKeyRange.only(userSettings.username));
            req.onsuccess = e => res(e.target.result || []);
            req.onerror = e => {
              console.error(`Failed to load user lists for ${userSettings.username}:`, e.target.error);
              rej(new Error(`Failed to load user lists: ${e.target.error?.message}`));
            };
          }));
        } else {
          listPromises.push(Promise.resolve([]));
        }

        // Wait for list results
        Promise.all(listPromises).then(([defaultListsResult, userListsResult]) => {
          const tempMap = {};
          (defaultListsResult || []).forEach(listMeta => { tempMap[listMeta.list] = listMeta; });
          (userListsResult || []).forEach(listMeta => { if (listMeta.username !== 'default') tempMap[listMeta.list] = listMeta; });
          listsMap = tempMap; // Assign to global state

          // Fix: Don't set activeLists here - it should start empty
          activeLists = []; // Start with no active filters, not all available lists

          console.log("List configurations loaded.");
          resolve(); // Resolve the outer list promise
        }).catch(reject);

        transactionLists.onerror = (event) => reject(`Lists transaction error: ${event.target.error?.message}`);
        transactionLists.onabort = (event) => reject(`Lists transaction aborted: ${event.target.error?.message}`);
      });
    } catch (listError) {
      // Catch errors related to loading lists
      console.error("Failed to load list configurations:", listError);
      // Set safe defaults and allow the app to continue if possible
      listsMap = {};
      activeLists = [];
    }

    // Check if core lists exist for this user and create if needed
    if (userSettings.username && userSettings.username !== 'default' && !setupNeeded) {
      // Check if core lists exist for this user
      const userCoreLists = await getUserLists(userSettings.username, "core");

      // If missing any core lists, create them from defaults
      if (userCoreLists.length < 4) { // Expected 4 core lists
        await setupUserCoreLists(userSettings.username);
        // Reload lists after creating them
        const updatedLists = await getUserLists(userSettings.username, "custom");
        // Update listsMap with the newly created lists
        updatedLists.forEach(list => {
          listsMap[list.list] = list;
        });
      }
    }

    // Initialize API Key Manager with loaded settings
    apiKeyManager.initialize(userSettings);

    // console.log(`loadCoreAppData returning: { setupNeeded: ${setupNeeded} }`);
    return { setupNeeded };

  } catch (error) {
    console.error("Critical error during loadCoreAppData:", error);
    userSettings = { requiresSetup: true };
    localStorage.removeItem('lastUsername');
    return { setupNeeded: true };
  }
}

async function setupUserCoreLists(newUsername) {
  try {
    // Get all default core lists
    const transaction = db.transaction(["lists_metadata"], "readonly");
    const metadataStore = transaction.objectStore("lists_metadata");
    const index = metadataStore.index("username_type");
    const defaultCoreLists = await (new Promise((resolve, reject) => {
      const request = index.getAll(IDBKeyRange.only(["default", "core"]));
      request.onsuccess = () => resolve(request.result || []);
      request.onerror = () => reject(request.error);
    }));

    // Clone each core list for the new user
    const writeTransaction = db.transaction(["lists_metadata"], "readwrite");
    const writeStore = writeTransaction.objectStore("lists_metadata");

    const promises = defaultCoreLists.map(template => {
      // Create a new core list for this user based on the template
      const userCoreList = {
        ...template,
        username: newUsername,
        created: Date.now(),
        lastUpdated: Date.now(),
        ratings: {},
        ratingsCount: 0,
        avgRating: 0
      };

      // Keep the critical properties that must remain unchanged
      // This ensures core lists behave consistently across all users
      userCoreList.type = "core";
      userCoreList.symbolId = template.symbolId;
      userCoreList.list = template.list;

      return new Promise((resolve, reject) => {
        const request = writeStore.add(userCoreList);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
      });
    });

    await Promise.all(promises);
    console.log(`Core lists created for new user: ${newUsername}`);

  } catch (error) {
    console.error("Failed to set up core lists for new user:", error);
    throw error;
  }
}

function registerServiceWorker() {
  if ('serviceWorker' in navigator && location.protocol !== 'file:') {
    // Check if document is already loaded
    if (document.readyState === 'complete') {
      // console.log('Document already loaded, registering immediately');
      registerSW();
    } else {
      // console.log('Waiting for load event before registering');
      window.addEventListener('load', registerSW);
    }
  } else {
    console.log('Service Workers not supported or running from file:// protocol');
  }

  function registerSW() {
    navigator.serviceWorker.getRegistration()
      .then(reg => {
        if (reg) {
          console.log('Service Worker registered with scope:', reg.scope);

          // Check if the service worker needs updating
          reg.update().then(() => {
            if (reg.waiting) {
              console.log('New service worker waiting to activate');
              // Notify user or handle update
            }
          });

          initializePeriodicCleanup(reg);
          return reg;
        }

        // Register new service worker
        return navigator.serviceWorker.register('sw.js')
          .then(registration => {
            console.log('Service Worker registered with scope:', registration.scope);
            initializePeriodicCleanup(registration);
            return registration;
          });
      })
      .catch(error => {
        console.warn('Service Worker registration failed:', error);
      });
  }

  function initializePeriodicCleanup(registration) {
    // Initialize periodic cleanup
    setInterval(() => {
      if (registration.active) {
        registration.active.postMessage({
          action: 'cleanupCache'
        });
      }
    }, 3600000); // Hourly cleanup
  }
}

function isTouchDevice() {
  return (
    'ontouchstart' in window ||
    navigator.maxTouchPoints > 0 ||
    navigator.msMaxTouchPoints > 0
  );
}

function handlePersonCardClick(_event, card) {
  if (isTouchDevice()) return;
  const imdbLink = card.getAttribute('data-imdb-link');
  if (imdbLink) {
    window.open(imdbLink, '_blank');
  }
}

function touchFilterHandler(e, el) {
  // Prevent duplicate handling (guard against both click and touchend firing)
  if (el._lastTouch && Date.now() - el._lastTouch < 500) return;
  el._lastTouch = Date.now();
  toggleListFilter(e, el);
}

function touchWatchedHandler(e, el) {
  if (el._lastTouch && Date.now() - el._lastTouch < 500) return;
  el._lastTouch = Date.now();
  cycleWatchedFilter(e, el);
}

async function showCustomListPanel(mode = 'list', lists = null) {
  const panel = document.getElementById('customlist-dropdown-panel');
  if (!panel) return;

  // Prevent bubbling to document
  panel.onclick = (e) => e.stopPropagation();

  // Load lists if not provided
  if (!lists) {
    lists = (await getUserLists(userSettings.username, 'custom')) || [];
  }

  async function reloadListPanel() {
    const freshLists = await getUserLists(userSettings.username, 'custom');
    // Update the global customListsMeta variable
    customListsMeta = freshLists;
    showCustomListPanel('list', freshLists);
}

  // Helper to close panel
  function closePanel() {
    panel.classList.add('hidden');
    updateFilterButtonState(document.getElementById('customlist-filter-icon'));
  }

  // Attach outside click only once
  if (!panel._outsideClickAttached) {
    document.addEventListener('click', (e) => {
      const icon = document.getElementById('customlist-filter-icon');
      if (
        !panel.classList.contains('hidden') &&
        !panel.contains(e.target) &&
        e.target !== icon
      ) {
        closePanel();
      }
    });
    panel._outsideClickAttached = true;
  }

  panel.innerHTML = ''; // Clear previous content

  if (mode === 'list') {
    // Compute item counts for each list
    const counts = {};
    for (const list of lists) {
      let count = 0;
      for (const media of mediaCache.values()) {
        if (media.lists && media.lists[list.list]) count++;
      }
      counts[list.list] = count;
    }

    // Pills grid (2 columns)
    panel.innerHTML = `
      <!--div class="customlist-panel-title">Custom Lists</div-->
      <div class="customlist-panel-content">
        <div class="customlist-pill-grid">
          ${lists.map(list => `
            <div class="genre-pill customlist-pill${activeLists.some(item => item.name === list.list && item.type === 'custom') ? ' active' : ''}"
                 data-list="${list.list}"
                 data-label="${list.label}"
                 tabindex="0"
                 data-tooltip>
              <span class="pill-edit-zone" aria-label="Edit">
                <svg class="pill-edit-icon" viewBox="0 0 24 24"><use href="#edit_list" /></svg>
              </span>
              <span class="pill-label">
                ${escapeHTML(list.label)}&nbsp;
                <span class="pill-count">(${counts[list.list] || 0})</span>
              </span>
              <span class="pill-delete-zone" aria-label="Delete">
                ×
              </span>
              <span class="ttip-txt" data-tt-pos="bottom">${escapeHTML(list.description || 'No description')}</span>
            </div>
          `).join('')}
          <div class="genre-pill customlist-pill add-pill" tabindex="0" data-tooltip>
            <span class="pill-add-icon">+</span><span class="ttip-txt" data-tt-pos="bottom">Add New Custom List</span>
          </div>
        </div>
      </div>
    `;

    // Add pill event (attach only to the add-pill, not all pills)
    const addPill = panel.querySelector('.add-pill');
    if (addPill) {
      addPill.addEventListener('click', (e) => {
        e.stopPropagation();
        showCustomListPanel('add', lists);
      });
    }

    // Pill click handlers for all pills except add
    panel.querySelectorAll('.customlist-pill').forEach(pill => {
      if (pill.classList.contains('add-pill')) return; // skip the add pill

      pill.addEventListener('click', (e) => {
        if (e.target.closest('.pill-edit-zone') || e.target.closest('.pill-delete-zone')) return;
        
        const listId = pill.dataset.list;
        // Check if the list still exists in customListsMeta
        const listExists = customListsMeta.some(list => list.list === listId);
        
        if (!listExists) {
          // Remove from activeLists if it doesn't exist
          const idx = activeLists.findIndex(item => item.name === listId && item.type === 'custom');
          if (idx !== -1) {
            activeLists.splice(idx, 1);
          }
          showNotification(`List "${pill.dataset.label}" no longer exists.`, false);
          reloadListPanel();
          return;
        }
        
        const idx = activeLists.findIndex(item => item.name === listId && item.type === 'custom');
        if (idx === -1) {
          activeLists.push({ name: listId, type: 'custom' });
          pill.classList.add('active');
        } else {
          activeLists.splice(idx, 1);
          pill.classList.remove('active');
        }
        
        // Re-apply filters and update icon state
        applyFiltersAndSearch();
        updateFilterButtonState(document.getElementById('customlist-filter-icon'));
      });

      // Edit zone
      const editZone = pill.querySelector('.pill-edit-zone');
      if (editZone) {
        editZone.addEventListener('click', (e) => {
          e.stopPropagation();
          showCustomListPanel('edit', [lists.find(l => l.list === pill.dataset.list)]);
        });
      }

      // Delete zone
      const deleteZone = pill.querySelector('.pill-delete-zone');
      if (deleteZone) {
        deleteZone.addEventListener('click', async (e) => {
          e.stopPropagation();
          const listId = pill.dataset.list;
          const listLabel = pill.dataset.label || "this list";
          const msg = `Delete <b>${escapeHTML(listLabel)}</b>?<br><br>This cannot be undone.`;
          const confirmed = await showCustomConfirm(msg);
          if (confirmed) {
            try {
              await deleteUserList(userSettings.username, listId);
              await reloadListMetadata(userSettings.username);
              await loadListStates();
              refreshCustomListsInTooltips();  
              showNotification('List deleted.', false);
              reloadListPanel();
            } catch (err) {
              showNotification('Error deleting list: ' + err.message, false);
            }
          }
        });
      }
    });
  } else if (mode === 'add') {
    panel.innerHTML = `
      <div class="customlist-panel-title">Add List</div>
      <div class="customlist-panel-content">
        <input id="custom-list-title" class="popup-input" type="text" maxlength="50" placeholder="List Title" autofocus>
        <textarea id="custom-list-desc" class="popup-input" rows="3" maxlength="200" placeholder="Description"></textarea>
        <div class="button-group">
          <button id="custom-list-add" class="popup-button">Add</button>
          <button id="custom-list-cancel" class="popup-button cancel-button">Cancel</button>
        </div>
      </div>
    `;
    panel.querySelector('#custom-list-cancel').onclick = (e) => {
      e.stopPropagation();
      reloadListPanel();
    };
    panel.querySelector('#custom-list-add').onclick = async (e) => {
      e.stopPropagation();
      const title = panel.querySelector('#custom-list-title').value.trim();
      const desc = panel.querySelector('#custom-list-desc').value.trim();
      if (!title) {
        showNotification('Please enter a list title.', false);
        return;
      }
      try {
        await createUserList(userSettings.username, title, desc, false);
        await reloadListMetadata(userSettings.username);
        await loadListStates();
        refreshCustomListsInTooltips(); 
        showNotification('List created!', false);
        reloadListPanel();
      } catch (err) {
        showNotification('Error saving list: ' + err.message, false);
      }
    };
  } else if (mode === 'edit') {
    // You can implement your edit UI here as needed
    const list = lists && lists[0];
    panel.innerHTML = `
      <div class="customlist-panel-title">Edit List</div>
      <div class="customlist-panel-content">
        <input id="custom-list-title" class="popup-input" type="text" maxlength="50" value="${escapeHTML(list?.label || '')}" placeholder="List Title" autofocus>
        <textarea id="custom-list-desc" class="popup-input" rows="3" maxlength="200" placeholder="Description">${escapeHTML(list?.description || '')}</textarea>
        <div class="button-group">
          <button id="custom-list-save" class="popup-button">Save</button>
          <button id="custom-list-cancel" class="popup-button cancel-button">Cancel</button>
        </div>
      </div>
    `;
    panel.querySelector('#custom-list-cancel').onclick = (e) => {
      e.stopPropagation();
      reloadListPanel();
    };
    panel.querySelector('#custom-list-save').onclick = async (e) => {
      e.stopPropagation();
      const title = panel.querySelector('#custom-list-title').value.trim();
      const desc = panel.querySelector('#custom-list-desc').value.trim();
      if (!title) {
        showNotification('Please enter a list title.', false);
        return;
      }
      try {
        await updateUserList(userSettings.username, list.list, title, desc);
        await reloadListMetadata(userSettings.username);
        await loadListStates();
        refreshCustomListsInTooltips();  
        showNotification('List updated!', false);
        reloadListPanel();
      } catch (err) {
        showNotification('Error saving list: ' + err.message, false);
      }
    };
  }
  panel.classList.remove('hidden');
}

function isCoreList(listName) {
  if (!listName) return false;
  
  // Check against all core list keys and labels (case-insensitive)
  const lower = listName.trim().toLowerCase();
  
  // Check keys
  for (const key in iconListConfig) {
    const config = iconListConfig[key];
    if (config.type === 'core') {
      if (key.toLowerCase() === lower) return true;
      // Also check the label
      if (config.label && config.label.toLowerCase() === lower) return true;
    }
  }
  
  return false;
}

/** 
 * Helper to generate UUIDs for custom lists
 */
function generateListId() {
  // Fast, standards-compliant UUID v4
  return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>
    (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)
  );
}

// Function to handle app installed state
function handleAppInstalled() {
  // Hide the install button
  const installButton = document.getElementById('install-button');
  if (installButton) {
    installButton.classList.add('hidden');
  }
  deferredPrompt = null;
}

/**
 * MAJOR DOM
 */
document.addEventListener("DOMContentLoaded", async () => {
  try {
    // Initialize user settings and core functionality
    await initializeApplication();

  } catch (error) {
    console.error("Error during initialization:", error);
  }

  // Check if Backup Schedule is due
  await runScheduledBackup(db, userSettings);

  // Map of click handlers for different elements
  const clickHandlers = {

    // List state toggles
    '[data-list-type]': handleIconClick,

    // Media actions
    '.delete-media': handleDeleteMedia,
    '.note-media': handleNoteMedia,
    '.refresh-media': handleRefreshMedia,
    '.detail-panel': handleDetailView,
    '.ttip-list-entry': async (event, element) => {
      event.stopPropagation();
      const imdbID = element.dataset.imdbid;
      const listId = element.dataset.list;
      if (!imdbID || !listId) return;
      try {
        await toggleListMembership(imdbID, listId, userSettings.username);
        element.classList.toggle('active');
        refreshCustomListsInTooltips(imdbID);
      } catch {
        showNotification('Failed to update list membership.', false);
      }
    },

    // Play button and URL links - these need to come BEFORE media-card
    '.play-link, .url-line a.full-link, .justwatch-link, .list-media': (event) => {
      event.stopPropagation(); // Always prevent bubbling to .media-card
    },

    '[data-video-key]': (_event, element) => {
      const videoKey = element.getAttribute('data-video-key');
      if (videoKey) {
        // Optionally check for a flag or type if you need to distinguish
        openVideoModal(videoKey, element.classList.contains('card'));
      }
    },

    '.media-card': handleMediaCardClick,

    // UI controls
    '#logo-button': handleLogoClick,
    '#settings-button': handleSettingsClick,
    '#backup-button': handleBackupClick,
    'label[for="restore-file"]': handleRestoreClick,
    '#import-button': handleImportClick,

    // Dropdowns
    '[data-dropdown]': toggleDropdown,      
    '[class*="dropdown-item"]': handleDropdownSelection,

    // Filter buttons
    '#genre-filter-button': handleGenreButtonClick,
    '#year-filter-button': handleYearFilterButtonClick,
    '#watched-filter-icon': cycleWatchedFilter,
    '#favourites-filter-icon': toggleListFilter,
    '#watchlist-filter-icon': toggleListFilter,
    '#collection-filter-icon': toggleListFilter,
    '#customlist-filter-icon': (event) => {
      event.stopPropagation();
      const panel = document.getElementById('customlist-dropdown-panel');
      const icon = document.getElementById('customlist-filter-icon');
      if (!panel) return;

      // If panel is already visible, close it and update state
      if (!panel.classList.contains('hidden')) {
        panel.classList.add('hidden');
        updateFilterButtonState(icon);
        return;
      }

      // Otherwise, open and populate the panel, and set active (panel is open)
      getUserLists(userSettings.username, 'custom').then(lists => {
        showCustomListPanel('list', lists);
        if (icon) icon.classList.add('active');
      });
    },

    '[data-tmdb-url]': (event, element) => {
      event.preventDefault();
      event.stopPropagation();
      const url = element.getAttribute('data-tmdb-url');
      if (url) window.open(url, '_blank');
    },

    '.dept-quicklink': (event, element) => {
      event.preventDefault();
      const targetId = element.getAttribute('data-target-id');
      const target = document.getElementById(targetId);
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'start' });
      }
    },

      // Rating sort handlers
    '.filter-section:nth-child(1) .rating-filter-icon': () => toggleRatingSort('imdb'),
    '.filter-section:nth-child(2) .rating-filter-icon': () => toggleRatingSort('tmdb'),
    '.filter-section:nth-child(3) .rating-filter-icon': () => toggleRatingSort('metascore'),
    '.filter-section:nth-child(4) .rating-filter-icon': () => toggleRatingSort('rt'),

    // Add entry
    '#add-entry': handleAddEntry,

    // Navigation panel
    '#navPanel .nav-link': handleNavClick,

    // Image modal navigation
    '#prev-backdrop': handlePrevBackdrop,
    '#next-backdrop': handleNextBackdrop,
    '#toggle-active-poster': handlePosterToggle,
    '.card.backdrop': (_event, element) => {
      const filePath = element.getAttribute('data-file-path');
      const imdbID = element.getAttribute('data-imdbid');
      openImageModal(filePath, imdbID, 'backdrop');
    },
    '.card.poster': (_event, element) => {
      const filePath = element.getAttribute('data-file-path');
      const imdbID = element.getAttribute('data-imdbid');
      openImageModal(filePath, imdbID, 'poster');
    },

    // Generic close button handler
    '.close-modal': (event, element) => {
      event.stopPropagation();
      const popupType = element.getAttribute('data-popup-type') || 'generic';
      
      // Special handling for video type
      if (popupType === 'video') {
        closePopup('video', { fromDetailView: isFromDetailView() });
      } 
      // Settings type
      else if (popupType === 'settings') {
        closePopup(popupType, { initialJWConfig });
      }
      // All other types
      else {
        closePopup(popupType);
      }
    },

    '#about-button': handleAboutClick,
    '.card.person': handlePersonCardClick,
    '.card.season': handleSeasonCardClick,
    '.card.episode': handleEpisodeCardClick,
    'document': closeDropdownsOutsideClick

  };

  // Mouse handler maps
  const mouseoverHandlers = {
    '.card': handleCardHover,
    '.media-card:not(.no-transform)': handleResizedGridCardHover,
  };
  
  if (isTouchDevice()) {
    let lastTappedCard = null;
    let lastTapTime = 0;
    let lastTapX = 0, lastTapY = 0;
    let tapMoved = false;
    const DOUBLE_TAP_DELAY = 400; // ms
    const TAP_MOVE_TOLERANCE = 10; // px
  
    document.addEventListener('touchstart', function(e) {
      const card = e.target.closest('.card.person');
      if (!card) return;
      if (e.touches.length > 1) return; // ignore multi-touch
      tapMoved = false;
      lastTapY = e.touches[0].clientY;
      lastTapX = e.touches[0].clientX;
    }, { passive: true });
  
    document.addEventListener('touchmove', function(e) {
      if (e.touches.length > 1) return;
      const dy = Math.abs(e.touches[0].clientY - lastTapY);
      const dx = Math.abs(e.touches[0].clientX - lastTapX);
      if (dx > TAP_MOVE_TOLERANCE || dy > TAP_MOVE_TOLERANCE) {
        tapMoved = true;
      }
    }, { passive: true });
  
    document.addEventListener('touchend', function(e) {
      const card = e.target.closest('.card.person');
      if (!card) return;
      if (tapMoved) return; // treat as scroll, not tap
  
      const now = Date.now();
  
      // First tap or tapping a different card: show tooltip
      if (lastTappedCard !== card || !card.classList.contains('tooltip-active')) {
        document.querySelectorAll('.card.person.tooltip-active').forEach(el => {
          el.classList.remove('tooltip-active');
        });
        card.classList.add('tooltip-active');
        lastTappedCard = card;
        lastTapTime = now;
        return;
      }
  
      // Tapping same card again
      if (card.classList.contains('tooltip-active')) {
        if ((now - lastTapTime) < DOUBLE_TAP_DELAY) {
          // Double-tap: open IMDb link
          const imdbLink = card.getAttribute('data-imdb-link');
          if (imdbLink) {
            card.classList.remove('tooltip-active');
            lastTappedCard = null;
            window.open(imdbLink, '_blank');
            e.preventDefault(); // prevent double-tap-to-zoom
          }
        } else {
          // Slow second tap: close tooltip
          card.classList.remove('tooltip-active');
          lastTappedCard = null;
          lastTapTime = 0;
        }
        return;
      }
    }, { passive: false }); // passive:false needed for preventDefault
  }

  // Single delegated click handler for the entire app
  document.addEventListener('click', (event) => {
    let handled = false;

    for (const [selector, handler] of Object.entries(clickHandlers)) {
      if (selector === 'document' && !handled) {
        handler(event);
        continue;
      }

      const element = event.target.closest(selector);
      if (element) {
        handler(event, element);
        handled = true;
        break;
      }
    }

    if (!handled) {
      closeDropdownsOutsideClick(event);
    }
  });

  // For list filters
  ['#favourites-filter-icon', '#watchlist-filter-icon', '#collection-filter-icon']
    .forEach(selector => setupFilterIconHandlers(selector, toggleListFilter));

  // For watched filter
  setupFilterIconHandlers('#watched-filter-icon', cycleWatchedFilter);

  // mouseover event listener
  document.addEventListener('mouseover', (event) => {
    // Safety check for closest method
    if (!event.target || typeof event.target.closest !== 'function') return;

    for (const [selector, handler] of Object.entries(mouseoverHandlers)) { 
      const element = event.target.closest(selector);
      if (element) {
        // Add a check to simulate mouseenter (only trigger once when entering)
        const relatedTarget = event.relatedTarget;
        if (relatedTarget && element.contains(relatedTarget)) continue;

        handler(event, element);
        break;
      }
    }
  });

  document.addEventListener('keydown', (event) => {
    // Ignore key events when typing in input or textarea
    if (event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA') return;

    // References to popups
    const videoModal = document.getElementById("videoModal");
    const imageModal = document.getElementById("imageModal");
    const popup = document.getElementById("popup");

    // Normalize the key
    const key = event.key.toLowerCase();

    switch (key) {
      case "x":
      case "escape":
      case "esc":
        // ESC/X: Close popups in order
        if (videoModal && videoModal.classList.contains("active")) {
          closePopup('video', { fromDetailView: isFromDetailView() });
          event.preventDefault();
          event.stopPropagation();
          break;
        }
        if (imageModal && !imageModal.classList.contains("hidden")) {
          closePopup('image');
          event.preventDefault();
          event.stopPropagation();
          break;
        }
        if (popup && !popup.classList.contains("hidden")) {
          let popupType = popup.getAttribute('data-popup-type') ||
            popup.querySelector('[data-popup-type]')?.getAttribute('data-popup-type');
          if (popupType) {
            if (popupType === 'settings') {
              closePopup('settings', { initialJWConfig });
            } else {
              closePopup(popupType);
            }
          } else {
            closePopup('about');
          }
          event.preventDefault();
          event.stopPropagation();
        }
        break;

      case "d":
        // Detail View
        {
          const hoveredCard = document.querySelector('.media-card:hover');
          if (hoveredCard) handleDetailView(event, hoveredCard);
        }
        break;

      case "r":
        // Refresh Media Card
        {
          const detailView = document.querySelector('.popup-content .detail-view');
          if (detailView) {
            handleRefreshMedia(event, detailView);
          } else {
            const hoveredCard = document.querySelector('.media-card:hover');
            if (hoveredCard) {
              handleRefreshMedia(event, hoveredCard);
            } else {
              showNotification('⚠️ No media to refresh.', false);
            }
          }
        }
        break;

      case "t":
        // Play Trailer
        {
          const detailView = document.querySelector('.popup-content .detail-view');
          let imdbID;
          if (detailView) {
            imdbID = detailView.dataset.imdbid;
          } else {
            const hoveredCard = document.querySelector('.media-card:hover');
            if (hoveredCard) imdbID = hoveredCard.dataset.imdbid;
          }
          if (!imdbID) {
            showNotification('⚠️ No media selected for trailer.', false);
            break;
          }
          const mediaData = mediaCache.get(imdbID);
          if (mediaData?.trailers?.length > 0) {
            openVideoModal(mediaData.trailers[0].key, !!detailView);
          } else {
            showNotification('⚠️ No trailer available for this media.', false);
          }
        }
        break;

      case "n": {
        const detailView = document.querySelector('.popup-content .detail-view');
        let imdbID, title;
        if (detailView) {
          imdbID = detailView.dataset.imdbid;
          title = detailView.dataset.mediaTitle || 'Unknown Title';
        } else {
          const hoveredCard = document.querySelector('.media-card:hover');
          if (hoveredCard) {
            imdbID = hoveredCard.dataset.imdbid;
            title = hoveredCard.dataset.mediaTitle || 'Unknown Title';
          }
        }
        if (imdbID) {
          showNoteEditorPopup(imdbID, title);
        } else {
          showNotification('⚠️ No media selected for note.', false);
        }
        break;
      }

      // Add more key cases as needed

      default:
        // No action
        break;
    }
  });

  window.addEventListener('beforeinstallprompt', (e) => {
    // Prevent Chrome from automatically showing the prompt
    e.preventDefault();

    // Stash the event so it can be triggered later
    deferredPrompt = e;

    // Get the install button
    const installButton = document.getElementById('install-button');
    if (installButton) {
      // Make it visible
      installButton.classList.remove('hidden');

      // Clear any existing event listeners to prevent duplicates
      const newButton = installButton.cloneNode(true);
      installButton.parentNode.replaceChild(newButton, installButton);

      // Add click event listener to the new button
      newButton.addEventListener('click', () => {
        // Hide the button
        newButton.classList.add('hidden');

        // Show the prompt
        deferredPrompt.prompt();

        // Wait for the user to respond to the prompt
        deferredPrompt.userChoice.then((choiceResult) => {
          if (choiceResult.outcome === 'accepted') {
            console.log('User accepted the install prompt');
            deferredPrompt = null;
          } else {
            console.log('User dismissed the install prompt');
            // Show the button again after a delay
            setTimeout(() => {
              if (deferredPrompt) {
                newButton.classList.remove('hidden');
              }
            }, 3000);
          }
        });
      });
    }
  });

  // Listen for the app being installed
  window.addEventListener('appinstalled', handleAppInstalled);

// Check if running as installed PWA on page load
  if (window.matchMedia('(display-mode: standalone)').matches ||
    window.navigator.standalone === true) {
    // App is running as installed PWA, no need for install button
    handleAppInstalled();
  }
});