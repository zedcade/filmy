# Defensive Layers: Bug Fixes Report

**Date**: February 16, 2026  
**Status**: 6 Critical Issues Identified  
**Priority**: Fix before major refactoring

---

## Executive Summary

The defensive layer classes (DataValidator, SafeIndexedDBOperation, SafeDOMOperation) are **well-architected but contain 6 critical bugs** that prevent full production use. These bugs don't affect current code because the _safe functions are not actively used, but they WILL cause failures if migration occurs.

**Recommendation**: Fix these bugs before any attempt to adopt _safe function patterns.

---

## BUG #1: SafeIndexedDBOperation.write() - Promise Doesn't Complete

**Severity**: 🔴 CRITICAL  
**File**: js/filmy.js  
**Lines**: 627-695  
**Impact**: Hangs indefinitely on transaction abort; retry logic unreachable

### Current Code (BROKEN)
```javascript
static async write(db, storeName, data, mode = 'put') {
  for (let attempt = 0; attempt < this.MAX_RETRIES; attempt++) {
    try {
      return new Promise((resolve, reject) => {  // ❌ RETURNS IMMEDIATELY
        const transaction = db.transaction([storeName], 'readwrite');
        // ...
        transaction.onabort = () => reject(new Error('Transaction aborted'));
      });
      // ❌ This catch never fires for Promise rejections
    } catch (error) {
      if (isAbortError && attempt < MAX_RETRIES - 1) {
        await delay(...);
        continue;  // ❌ UNREACHABLE - Promise rejection doesn't throw
      }
    }
  }
}
```

### Root Cause
- `return new Promise()` immediately returns to caller
- Promise rejection inside Promise constructor doesn't throw
- Catch block is unreachable
- Retry loop never executes

### Fixed Code
```javascript
static async write(db, storeName, data, mode = 'put') {
  for (let attempt = 0; attempt < this.MAX_RETRIES; attempt++) {
    try {
      // ✅ Await the promise so rejections can be caught
      const result = await new Promise((resolve, reject) => {
        const transaction = db.transaction([storeName], 'readwrite');
        const store = transaction.objectStore(storeName);
        const request = mode === 'add' ? store.add(data) : store.put(data);

        request.onsuccess = () => resolve(true);
        request.onerror = () => reject(new Error(`Request error: ${request.error?.message || 'unknown'}`));
        transaction.onerror = () => reject(new Error(`Transaction error: ${transaction.error?.message || 'unknown'}`));
        transaction.onabort = () => reject(new Error('Transaction aborted'));
      });
      return result;  // ✅ Return successful result
    } catch (error) {
      const isQuotaError = error.name === 'QuotaExceededError';
      const isAbortError = error.message === 'Transaction aborted';

      if (isQuotaError) {
        this.log('error', 'Storage quota exceeded', { storeName });
        this.handleQuotaExceeded(db);
        return false;
      }

      // ✅ NOW THIS WORKS - Catch block is reachable
      if (isAbortError && attempt < this.MAX_RETRIES - 1) {
        this.log('warn', `Write aborted, retrying (attempt ${attempt + 1}/${this.MAX_RETRIES})`, { storeName });
        await this.delay(this.RETRY_DELAY_MS * Math.pow(2, attempt));
        continue;  // ✅ Retry loop now executes
      }

      this.log('error', `Write failed for ${storeName} (attempt ${attempt + 1}/${this.MAX_RETRIES})`, { 
        error: error.message,
        isDuplicateKeyError: error.message.includes('ConstraintError'),
        mode,
        dataKeys: typeof data === 'object' ? Object.keys(data) : typeof data
      });

      if (attempt === this.MAX_RETRIES - 1) {
        return false;
      }

      // ✅ Exponential backoff instead of linear
      await this.delay(this.RETRY_DELAY_MS * Math.pow(2, attempt));
    }
  }

  return false;
}
```

### Impact Without Fix
- Transactions that abort never retry
- Storage quota errors silently fail instead of triggering cleanup
- App appears to freeze when database is under stress

---

## BUG #2: DataValidator.validateOMDbMedia() - Error Detection Order Wrong

**Severity**: 🔴 CRITICAL  
**File**: js/filmy.js  
**Lines**: 443-529  
**Impact**: Validates missing fields on error responses; inaccurate error reporting

### Current Code (BROKEN)
```javascript
static validateOMDbMedia(item) {
  const errors = [];
  const sanitized = { ...item };

  // Check for API error
  if (sanitized.Error) {
    // ✅ Correctly returns error
    errors.push(`OMDb Error: ${sanitized.Error}`);
    return { isValid: false, errors, sanitized };
  }

  // Required fields - ❌ These checks run even if Response='False'
  if (sanitized.Response !== 'True') {
    errors.push('OMDb Response not True');  // ❌ Vague
  }

  if (!sanitized.imdbID) {
    errors.push('Missing imdbID');  // ❌ Will be true on error responses
  }
  // ... validate other fields
}
```

### Problem Scenario
OMDb returns: `{ Response: 'False', Error: 'Movie not found!', imdbID: undefined }`

Current behavior:
1. Early return catches Error ✅
2. BUT if Error check is skipped, Response !== 'True' adds vague error
3. Then it validates imdbID (which is undefined on error response)
4. Multiple confusing error messages

### Fixed Code
```javascript
static validateOMDbMedia(item) {
  if (!item || typeof item !== 'object') {
    return { 
      isValid: false, 
      errors: ['Empty or invalid OMDb response'], 
      sanitized: {} 
    };
  }

  const errors = [];
  const sanitized = { ...item };

  // ✅ Check for API error FIRST (most specific)
  if (sanitized.Error) {
    return { 
      isValid: false, 
      errors: [`OMDb Error: ${sanitized.Error}`],  // ✅ Clear error message
      sanitized 
    };
  }

  // ✅ Check Response status (second most specific)
  if (sanitized.Response !== 'True') {
    return { 
      isValid: false, 
      errors: [`OMDb Response not True: ${sanitized.Response}`],  // ✅ Include actual value
      sanitized 
    };
  }

  // ✅ Only validate fields if Response is True
  if (!sanitized.imdbID) {
    errors.push('Missing imdbID');
  }

  // Parse ratings, year, metascore only if Response='True'
  if (sanitized.imdbRating && sanitized.imdbRating !== 'N/A') {
    const rating = parseFloat(sanitized.imdbRating);
    if (!Number.isFinite(rating)) {
      errors.push(`Invalid imdbRating: ${sanitized.imdbRating}`);
      sanitized.imdbRating = 'N/A';
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    sanitized
  };
}
```

### Impact Without Fix
- Error responses pollute error log with multiple confusing messages
- Hard to diagnose why OMDb lookups failed
- Invalid data might slip through if error checking is skipped

---

## BUG #3: SafeIndexedDBOperation.writeBatch() - Poor Error Reporting

**Severity**: 🟠 HIGH  
**File**: js/filmy.js  
**Lines**: 709-739  
**Impact**: Unclear which items failed and why; mode parameter affects debugging

### Current Code (BROKEN)
```javascript
static async writeBatch(db, storeName, items, mode = 'put') {
  if (!db || !Array.isArray(items)) {
    return { successful: 0, failed: items.length, errors: [] };  // ❌ items might be null
  }

  const results = { successful: 0, failed: 0, errors: [], failedIndices: [] };

  for (let i = 0; i < items.length; i++) {
    const success = await this.write(db, storeName, items[i], mode);
    if (success) {
      results.successful++;
    } else {
      results.failed++;
      results.failedIndices.push(i);
      results.errors.push(new Error(`Failed to write item at index ${i}`));
      // ❌ Error doesn't indicate WHY it failed or if mode='add'
      // ❌ Duplicate key vs quota vs transport error all look the same
    }
  }
  return results;
}
```

### Fixed Code
```javascript
static async writeBatch(db, storeName, items, mode = 'put') {
  if (!db) {
    return { successful: 0, failed: (items?.length || 0), errors: ['Database not initialized'], failedIndices: [] };
  }

  if (!Array.isArray(items)) {
    return { successful: 0, failed: 0, errors: ['Items is not an array'], failedIndices: [] };
  }

  if (items.length === 0) {
    return { successful: 0, failed: 0, errors: [], failedIndices: [] };
  }

  const results = { successful: 0, failed: 0, errors: [], failedIndices: [] };

  for (let i = 0; i < items.length; i++) {
    const success = await this.write(db, storeName, items[i], mode);
    if (success) {
      results.successful++;
    } else {
      results.failed++;
      results.failedIndices.push(i);
      // ✅ Error now includes mode context
      const modeStr = mode === 'add' ? 'add (possible duplicate key)' : 'put';
      results.errors.push(new Error(`Failed to ${modeStr} item at index ${i}`));
    }
  }

  if (results.failed > 0) {
    this.log('warn', `Batch write: ${results.successful}/${items.length} succeeded, ${results.failed} failed`, {
      storeName,
      mode,
      failedIndices: results.failedIndices  // ✅ Log which ones failed
    });
  } else if (items.length > 0) {
    this.log('info', `Batch write: all ${results.successful} items succeeded`, { storeName });
  }

  return results;
}
```

---

## BUG #4: SafeDOMOperation.setHTML() - Incomplete XSS Protection

**Severity**: 🔴 CRITICAL  
**File**: js/filmy.js  
**Lines**: 1029-1054  
**Impact**: XSS vulnerabilities through event handlers, iframes, SVG exploits

### Current Code (BROKEN)
```javascript
static setHTML(element, html) {
  const temp = document.createElement('div');
  temp.innerHTML = html;
  const scripts = temp.querySelectorAll('script');
  scripts.forEach(script => script.remove());  // ❌ Only removes <script> tags

  el.innerHTML = temp.innerHTML;  // ❌ Still vulnerable to:
  // <img src=x onerror="alert('xss')">
  // <iframe src="javascript:alert('xss')"></iframe>
  // <svg onload="alert('xss')"></svg>
  // <style>*{background:url('javascript:alert(1)')}</style>
}
```

### Vulnerability Examples
```html
<!-- Not prevented -->
<img src=x onerror="alert('xss')">
<svg onload="alert('xss')">
<iframe src="javascript:alert('xss')"></iframe>
<body onload="alert('xss')">
<div style="background:url('javascript:alert(1)')">
```

### Fixed Code
```javascript
static setHTML(element, html) {
  try {
    const el = typeof element === 'string' ? this.querySelector(element) : element;
    if (!el) {
      this.warn('setHTML: element not found', { element });
      return false;
    }

    const temp = document.createElement('div');
    temp.innerHTML = html;
    
    // ✅ Remove ALL dangerous tags
    const dangerousTags = ['script', 'iframe', 'object', 'embed', 'link', 'style'];
    dangerousTags.forEach(tag => {
      temp.querySelectorAll(tag).forEach(el => el.remove());
    });
    
    // ✅ Remove ALL event handler attributes and dangerous protocols
    temp.querySelectorAll('*').forEach(node => {
      const attrsToRemove = [];
      Array.from(node.attributes || []).forEach(attr => {
        if (attr.name.startsWith('on') ||  // Block onclick, onerror, etc.
            (attr.name === 'src' && attr.value.startsWith('javascript:')) ||
            (attr.name === 'href' && attr.value.startsWith('javascript:'))) {
          attrsToRemove.push(attr.name);
        }
      });
      attrsToRemove.forEach(name => node.removeAttribute(name));
    });

    el.innerHTML = temp.innerHTML;
    return true;
  } catch (error) {
    this.error('setHTML failed', { error: error.message, element });
    return false;
  }
}
```

**Note**: For user-generated content, consider using DOMPurify library instead. This fix handles TMDB/OMDb responses which are trusted sources.

---

## BUG #5: DataValidator.validateBatch() - Silent Failures on Invalid Input

**Severity**: 🟠 HIGH  
**File**: js/filmy.js  
**Lines**: 520-562  
**Impact**: Returns empty results without warning when input is invalid

### Current Code (BROKEN)
```javascript
static validateBatch(items, validator = 'tmdb-movie') {
  if (!Array.isArray(items)) {
    return { total: 0, valid: 0, errors: new Map(), sanitized: [] };  // ❌ Silent fail
  }

  if (items.length === 0) {
    // ❌ Returns valid result for empty array, hard to distinguish from error
    return { total: 0, valid: 0, errors: new Map(), sanitized: [] };
  }

  // ... validation logic with no try/catch
  items.forEach((item, index) => {
    let result;
    if (validator === 'tmdb-movie') {
      result = this.validateTMDBMedia(item, 'movie');  // ❌ May throw
    }
    // ...
  });
}
```

### Fixed Code
```javascript
static validateBatch(items, validator = 'tmdb-movie') {
  if (!Array.isArray(items)) {
    return { 
      total: 0, 
      valid: 0, 
      invalid: 0,
      errors: new Map(), 
      sanitized: [],
      warning: 'Input is not an array'  // ✅ Explicit warning
    };
  }

  if (items.length === 0) {
    return { 
      total: 0, 
      valid: 0, 
      invalid: 0,
      errors: new Map(), 
      sanitized: []
      // ✅ No warning for empty array (expected case)
    };
  }

  const errors = new Map();
  const sanitized = [];
  let valid = 0;

  items.forEach((item, index) => {
    let result;
    try {  // ✅ Catch validator errors
      if (validator === 'tmdb-movie') {
        result = this.validateTMDBMedia(item, 'movie');
      } else if (validator === 'tmdb-tv') {
        result = this.validateTMDBMedia(item, 'tv');
      } else if (validator === 'omdb') {
        result = this.validateOMDbMedia(item);
      } else if (validator === 'episode') {
        result = this.validateEpisode(item);
      } else {
        result = { isValid: false, errors: [`Unknown validator: ${validator}`], sanitized: item };
      }
    } catch (error) {
      result = { 
        isValid: false, 
        errors: [`Validation error: ${error.message}`], 
        sanitized: item 
      };
    }

    if (result && result.isValid) {
      valid++;
    } else if (result) {
      errors.set(index, result.errors);
    }
    
    sanitized.push(result?.sanitized || item);
  });

  return {
    total: items.length,
    valid,
    invalid: items.length - valid,
    errors,
    sanitized
  };
}
```

---

## BUG #6: SafeDOMOperation.forEach() - No Error Collection for Retry

**Severity**: 🟡 MEDIUM  
**File**: js/filmy.js  
**Lines**: 1318-1338  
**Impact**: Error logging doesn't inform caller which elements failed

### Current Code (BROKEN)
```javascript
static forEach(selector, callback) {
  const elements = this.querySelectorAll(selector);
  elements.forEach((el, index) => {
    try {
      callback(el, index);
    } catch (error) {
      this.error(`forEach callback failed at index ${index}`, { error: error.message });
      // ❌ Error is logged but caller has no way to know which failed
      // ❌ Caller might want to retry just the failed ones
    }
  });

  return elements.length;  // ❌ Only returns count, not which ones failed
}
```

### Fixed Code
```javascript
static forEach(selector, callback) {
  try {
    if (typeof callback !== 'function') {
      this.warn('forEach: callback is not a function');
      return { processed: 0, errors: [] };  // ✅ Clear return structure
    }

    const elements = this.querySelectorAll(selector);
    const errors = [];  // ✅ Collect all errors

    elements.forEach((el, index) => {
      try {
        callback(el, index);
      } catch (error) {
        errors.push({ index, error: error.message });
        this.error(`forEach callback failed at index ${index}`, { error: error.message });
      }
    });

    if (errors.length > 0) {
      // ✅ Warn with failed indices for easy retry
      this.warn(`forEach: ${errors.length}/${elements.length} callbacks failed`, { 
        failedIndices: errors.map(e => e.index) 
      });
    }

    return { processed: elements.length, errors };  // ✅ Return both count AND errors
  } catch (error) {
    this.error('forEach failed', { error: error.message });
    return { processed: 0, errors: [{ index: -1, error: error.message }] };
  }
}
```

---

## Summary: Code Changes Required

| Bug | Function | Lines | Fix Type | Time |
|-----|----------|-------|----------|------|
| #1 | SafeIndexedDBOperation.write() | 627-695 | Replace try/return with await | 15 min |
| #2 | DataValidator.validateOMDbMedia() | 443-529 | Reorder checks, add early returns | 10 min |
| #3 | SafeIndexedDBOperation.writeBatch() | 709-739 | Better error reporting | 15 min |
| #4 | SafeDOMOperation.setHTML() | 1029-1054 | XSS protection expansion | 15 min |
| #5 | DataValidator.validateBatch() | 520-562 | Add try/catch, warnings | 10 min |
| #6 | SafeDOMOperation.forEach() | 1318-1338 | Return error collection | 15 min |

**Total Time to Fix**: ~80 minutes (~1.5 hours)

---

## Integration Path (When Ready)

### Phase 1: Fix bugs in-place (1.5 hours)
- Apply all 6 fixes above
- Run test suite (DefensiveLayerTests)
- Commit to git

### Phase 2: Adopt _Safe gradually (4-6 weeks)
Replace one major function at a time:
1. Week 1: `saveToDatabase()` → `saveToDatabase_Safe()` (20 call sites)
2. Week 2: `readFromDatabase()` → `readFromDatabase_Safe()` (15 call sites)
3. Week 3: Query operations (10 call sites)
4. Week 4+: DOM operations (optional, lower priority)

Each replacement:
- Search for call sites
- Update to _Safe version
- Run tests
- Verify in dev environment

### Phase 3: Decommission old functions (1 week)
- Remove original unsaf versions once all migrate
- Update documentation
- Train team on new patterns

---

## Maintenance Going Forward

Once fixes are applied:

```javascript
// Store access follow this pattern:
// ✅ GOOD (with error handling)
const result = await saveToDatabase_Safe(db, movieData, 'movies');
if (!result.success) {
  console.warn('Save failed:', result.errors);
  // Handle error appropriately
}

// ✅ GOOD (with validation)
const validation = DataValidator.validateTMDBMedia(apiResponse, 'movie');
if (!validation.isValid) {
  console.warn('Invalid data:', validation.errors);
  return; // Don't save
}

// ❌ OLD PATTERN (to phase out)
const transaction = db.transaction(['movies'], 'readwrite');
// ... no validation, potential for corruption
```

