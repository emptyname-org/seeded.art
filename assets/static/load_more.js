(() => {
  const MAX_BYTES = 3 * 1024 * 1024;
  const MAX_REGION_ROWS = 2400;
  const TIMEOUT_MS = 10000;
  const active = new Map();
  let generation = 0;
  let pageHidden = false;

  const regionFor = nav => nav.dataset.loadMore;
  const cursorKey = region => ({
    "search-projects": "projects_after",
    "search-artists": "artists_after",
    "search-collectors": "collectors_after",
    "search-articles": "articles_after",
    "profile-articles": "articles_after",
    "project-articles": "articles_after",
  })[region] || "after";
  const CURSOR_KEYS = ["after", "articles_after", "projects_after", "artists_after", "collectors_after"];

  function eligibleLink(nav) {
    return nav.querySelector('a[rel="next"]');
  }

  function statusFor(nav) {
    let status = nav.querySelector("[data-load-more-status]");
    if (!status) {
      status = document.createElement("span");
      status.dataset.loadMoreStatus = "";
      status.className = "load-more-status";
      status.setAttribute("role", "status");
      status.setAttribute("aria-live", "polite");
      nav.append(status);
    }
    return status;
  }

  function announce(nav, message) {
    statusFor(nav).textContent = message;
  }

  function setFirstHidden(nav, hidden) {
    const first = nav.querySelector('a[rel="first"]');
    if (first) first.hidden = hidden;
  }

  function enhance() {
    document.querySelectorAll("nav[data-load-more]").forEach(nav => {
      const link = eligibleLink(nav);
      if (!link) return;
      link.dataset.loadMoreLink = "";
      link.textContent = "Load more";
      // Earlier results stay on screen, so "First page" only matters for page-by-page navigation.
      setFirstHidden(nav, true);
    });
  }

  function requestUrl(href, region) {
    const url = new URL(href, location.href);
    if (url.origin !== location.origin || url.protocol !== location.protocol) {
      throw new Error("This page can only load more results from this site.");
    }
    const key = cursorKey(region);
    const current = new URL(location.href);
    if (current.pathname === url.pathname) {
      for (const other of CURSOR_KEYS) {
        if (other === key) continue;
        if (current.searchParams.has(other)) {
          url.searchParams.set(other, current.searchParams.get(other));
        } else {
          url.searchParams.delete(other);
        }
      }
    }
    return url;
  }

  function positionUrl(requested, region) {
    const result = new URL(location.href);
    const source = new URL(requested.href);
    const key = cursorKey(region);
    result.pathname = source.pathname;
    result.search = source.search;
    if (result.pathname === source.pathname) {
      const current = new URL(location.href);
      for (const other of CURSOR_KEYS) {
        if (other === key) continue;
        if (current.searchParams.has(other)) result.searchParams.set(other, current.searchParams.get(other));
        else result.searchParams.delete(other);
      }
    }
    if (source.hash) result.hash = source.hash;
    else if (region.startsWith("search-")) result.hash = `#${region}`;
    return result;
  }

  function mergeFutureHref(href, position, region) {
    const future = new URL(href, location.href);
    const current = new URL(position.href);
    const key = cursorKey(region);
    if (future.pathname === current.pathname) {
      for (const other of CURSOR_KEYS) {
        if (other === key) continue;
        if (current.searchParams.has(other)) future.searchParams.set(other, current.searchParams.get(other));
        else future.searchParams.delete(other);
      }
    }
    return future.href;
  }

  async function readBounded(response, signal) {
    const declared = Number(response.headers.get("content-length"));
    if (Number.isFinite(declared) && declared > MAX_BYTES) throw Object.assign(new Error("The next page is too large to load safely."), {nativeFallback: true});
    if (!response.body?.getReader) {
      const text = await response.text();
      if (new TextEncoder().encode(text).byteLength > MAX_BYTES) throw Object.assign(new Error("The next page is too large to load safely."), {nativeFallback: true});
      return text;
    }
    const reader = response.body.getReader();
    const chunks = [];
    let size = 0;
    try {
      while (true) {
        if (signal.aborted) throw signal.reason || new DOMException("Aborted", "AbortError");
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > MAX_BYTES) {
          await reader.cancel();
          throw Object.assign(new Error("The next page is too large to load safely."), {nativeFallback: true});
        }
        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }
    const joined = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) { joined.set(chunk, offset); offset += chunk.byteLength; }
    return new TextDecoder().decode(joined);
  }

  function rowKey(node) {
    for (const name of ["projectId", "editionKey", "articleId", "marketKey", "tokenId"]) {
      if (node.dataset[name]) return `${name}:${node.dataset[name]}`;
    }
    const link = node.matches("a[href]") ? node : node.querySelector("a[href]");
    if (link) {
      const url = new URL(link.getAttribute("href"), location.href);
      return `href:${url.pathname}${url.search}`;
    }
    const text = node.textContent.trim().replace(/\s+/g, " ");
    return text ? `text:${text}` : null;
  }

  function appendRows(liveRegion, fetchedRegion) {
    const existing = new Set([...liveRegion.children].map(rowKey).filter(Boolean));
    const additions = [];
    for (const row of fetchedRegion.children) {
      const key = rowKey(row);
      if (!key || existing.has(key)) continue;
      existing.add(key);
      row.querySelectorAll("script").forEach(script => script.remove());
      additions.push(document.importNode(row, true));
    }
    if (liveRegion.children.length + additions.length > MAX_REGION_ROWS) return { additions: [], overflow: true };
    for (const row of additions) liveRegion.append(row);
    return { additions, overflow: false };
  }

  function samePage(actual, requested) {
    if (actual.origin !== requested.origin || actual.pathname !== requested.pathname) return false;
    return true;
  }

  async function load(nav, link) {
    const region = regionFor(nav);
    if (!region || active.has(region) || pageHidden) return;
    let requested;
    try { requested = requestUrl(link.href, region); }
    catch (error) { announce(nav, error.message); return; }
    const controller = new AbortController();
    const requestGeneration = generation;
    const task = { controller, generation: requestGeneration };
    active.set(region, task);
    const timeout = setTimeout(() => controller.abort(new Error("The request timed out. Try loading more again.")), TIMEOUT_MS);
    nav.setAttribute("aria-busy", "true");
    link.setAttribute("aria-disabled", "true");
    announce(nav, "Loading more results…");
    let permanent = false;
    try {
      const response = await fetch(requested.href, {
        credentials: "same-origin",
        headers: { Accept: "text/html" },
        redirect: "follow",
        signal: controller.signal,
      });
      const finalUrl = new URL(response.url || requested.href, location.href);
      permanent = true;
      if (response.redirected || !samePage(finalUrl, requested)) throw new Error("The next page redirected. Use the page link to continue.");
      permanent = response.status >= 400 && response.status < 500 && ![408, 429].includes(response.status);
      if (!response.ok) throw new Error(response.status === 401 || response.status === 403 ? "Sign in before loading this page." : "Could not load more results. Try again.");
      permanent = true;
      if (!(response.headers.get("content-type") || "").toLowerCase().startsWith("text/html")) throw new Error("The next page did not return HTML. Try again.");
      permanent = false;
      const html = await readBounded(response, controller.signal);
      if (controller.signal.aborted || pageHidden || requestGeneration !== generation) return;
      const parsed = new DOMParser().parseFromString(html, "text/html");
      parsed.querySelectorAll("script").forEach(script => script.remove());
      const selector = `[data-load-more-region="${CSS.escape(region)}"]`;
      const liveRegion = document.querySelector(selector);
      const fetchedRegion = parsed.querySelector(selector);
      permanent = true;
      if (!liveRegion || !fetchedRegion) throw new Error("The next page did not contain this result list. Use its page link to continue.");
      const appended = appendRows(liveRegion, fetchedRegion);
      if (appended.overflow) {
        link.href = requested.href;
        link.textContent = "Next page";
        nav.removeAttribute("data-load-more");
        setFirstHidden(nav, false);
        announce(nav, "The display limit is reached. Use Next page to continue.");
        return;
      }
      const additions = appended.additions;
      const nextNav = [...parsed.querySelectorAll("nav[data-load-more]")].find(candidate => candidate.dataset.loadMore === region);
      const nextHref = nextNav?.querySelector('a[rel="next"]')?.getAttribute("href");
      const position = positionUrl(requested, region);
      if (nextHref) {
        link.href = mergeFutureHref(nextHref, position, region);
        link.removeAttribute("aria-disabled");
        announce(nav, additions.length ? `Added ${additions.length} more results.` : "No new results were found.");
      } else {
        const wasFocused = document.activeElement === link;
        link.remove();
        announce(nav, "End of results.");
        if (wasFocused) {
          const focusTarget = additions[0] || statusFor(nav);
          focusTarget.tabIndex = -1;
          focusTarget.focus({ preventScroll: true });
        }
      }
      history.replaceState({ ...history.state, loadMoreScrollY: scrollY }, "", position.href);
      enhance();
    } catch (error) {
      if ((permanent || error.nativeFallback) && !pageHidden && requestGeneration === generation) {
        nav.removeAttribute("data-load-more");
        link.textContent = "Next page";
        setFirstHidden(nav, false);
      }
      if (!pageHidden && requestGeneration === generation && !controller.signal.aborted) announce(nav, error.message || "Could not load more results. Try again.");
      else if (!pageHidden && requestGeneration === generation && controller.signal.reason?.message) announce(nav, controller.signal.reason.message);
    } finally {
      clearTimeout(timeout);
      if (active.get(region) === task) active.delete(region);
      if (!pageHidden && requestGeneration === generation) {
        nav.removeAttribute("aria-busy");
        if (link.isConnected) link.removeAttribute("aria-disabled");
      }
    }
  }

  document.addEventListener("click", event => {
    const link = event.target.closest('nav[data-load-more] a[rel="next"]');
    if (!link) return;
    if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    const nav = link.closest("nav[data-load-more]");
    event.preventDefault();
    if (!nav || link.getAttribute("aria-disabled") === "true") return;
    void load(nav, link);
  });

  function cancelForNavigation() {
    generation++;
    for (const request of active.values()) request.controller.abort(new DOMException("History position changed", "AbortError"));
    active.clear();
    document.querySelectorAll("nav[data-load-more][aria-busy]").forEach(nav => {
      nav.removeAttribute("aria-busy");
      nav.querySelectorAll("a[aria-disabled]").forEach(link => link.removeAttribute("aria-disabled"));
      announce(nav, "Loading stopped because the page position changed. You can retry.");
    });
  }
  window.addEventListener("popstate", cancelForNavigation);

  window.addEventListener("pagehide", () => {
    pageHidden = true;
    generation++;
    for (const request of active.values()) request.controller.abort(new DOMException("Page hidden", "AbortError"));
    active.clear();
  });
  window.addEventListener("pageshow", () => {
    pageHidden = false;
    document.querySelectorAll("nav[data-load-more]").forEach(nav => {
      nav.removeAttribute("aria-busy");
      nav.querySelectorAll("a[aria-disabled]").forEach(link => link.removeAttribute("aria-disabled"));
    });
    enhance();
  });
  enhance();
})();
