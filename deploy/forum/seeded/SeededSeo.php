<?php

// Description, Open Graph and discussion structured data for the home and
// discussion pages. No Flarum dependencies, so tests can load this file on its own.

namespace Seeded\Forum;

use DateTimeImmutable;
use Throwable;

final class SeededSeo
{
    public const SITE_NAME = 'seeded.art forum';
    private const EXCERPT = 160;

    /**
     * Wires a Flarum Document. Extension content runs before the route's own
     * content, so the tags are a head entry rendered once the page is complete.
     */
    public static function attach(object $document, string $path, mixed $route): void
    {
        $forum = $document->getForumApiDocument()['data']['attributes'] ?? [];
        $description = self::plain(self::str($forum['description'] ?? null)) ?: self::SITE_NAME;
        $title = trim(self::str($forum['title'] ?? null)) ?: self::SITE_NAME;
        if ($route === 'discussion') {
            // Replaced by the first post's excerpt at render time.
            unset($document->meta['description']);
        } elseif (rtrim($path, '/') === '') {
            $document->meta['description'] = $description;
        } else {
            return;
        }
        $document->head[] = new SeededSeoHead($document, $route !== 'discussion', $title, $description);
    }

    /** Plain text of rendered post HTML: no tags, entities decoded, whitespace collapsed. */
    public static function plain(string $html): string
    {
        $html = mb_scrub($html, 'UTF-8');
        $html = preg_replace('#<(script|style|template)\b.*?</\1\s*>#is', ' ', $html) ?? '';
        $html = preg_replace('#<(?:br|hr|/?(?:p|div|li|ul|ol|h[1-6]|blockquote|pre|table|tr|td|th|figure|figcaption|details|summary))\b[^>]*>#i', ' ', $html) ?? '';
        $text = html_entity_decode(strip_tags($html), ENT_QUOTES | ENT_HTML5, 'UTF-8');

        return trim(preg_replace('/[\s\p{Z}]+/u', ' ', $text) ?? '');
    }

    /** At most 160 characters of plain() text, cut at a word boundary with "…". */
    public static function excerpt(string $text): string
    {
        if (mb_strlen($text) <= self::EXCERPT) {
            return $text;
        }
        $space = mb_strrpos(mb_substr($text, 0, self::EXCERPT), ' ');
        $cut = $space === false ? '' : rtrim(mb_substr($text, 0, $space), ' ,;:.-');

        return ($cut === '' ? mb_substr($text, 0, self::EXCERPT - 1) : $cut).'…';
    }

    public static function attr(string $value): string
    {
        return htmlspecialchars($value, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
    }

    public static function meta(string $name, string $content): string
    {
        return '<meta name="'.self::attr($name).'" content="'.self::attr($content).'">';
    }

    /** Open Graph and Twitter card tags; empty values are left out. */
    public static function tags(string $type, string $title, string $description, string $url): string
    {
        $tags = [];
        $properties = ['og:site_name' => self::SITE_NAME, 'og:type' => $type, 'og:title' => $title, 'og:description' => $description, 'og:url' => $url];
        foreach ($properties as $property => $content) {
            if ($content !== '') {
                $tags[] = '<meta property="'.$property.'" content="'.self::attr($content).'">';
            }
        }
        $tags[] = self::meta('twitter:card', 'summary');

        return implode("\n", $tags);
    }

    /** JSON-LD script; <, > and & are escaped so post text cannot close the script. */
    public static function jsonLd(array $data): string
    {
        $json = json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_HEX_TAG | JSON_HEX_AMP | JSON_INVALID_UTF8_SUBSTITUTE | JSON_THROW_ON_ERROR);

        return '<script type="application/ld+json">'.$json.'</script>';
    }

    /**
     * Fields of a Flarum JSON:API discussion document, or null for other documents.
     * html is the first post's contentHtml, null when that post is not on this page.
     */
    public static function discussion(array $api): ?array
    {
        $data = $api['data'] ?? null;
        if (! is_array($data) || ($data['type'] ?? null) !== 'discussions') {
            return null;
        }
        $attributes = is_array($data['attributes'] ?? null) ? $data['attributes'] : [];
        $authorId = self::str($data['relationships']['user']['data']['id'] ?? null);
        $author = '';
        $html = null;
        foreach (is_array($api['included'] ?? null) ? $api['included'] : [] as $item) {
            $fields = is_array($item['attributes'] ?? null) ? $item['attributes'] : [];
            $type = $item['type'] ?? null;
            if ($type === 'users' && $authorId !== '' && self::str($item['id'] ?? null) === $authorId) {
                $author = trim(self::str($fields['displayName'] ?? $fields['username'] ?? null));
            } elseif ($type === 'posts' && ($fields['number'] ?? null) === 1 && ($fields['contentType'] ?? null) === 'comment'
                && empty($fields['isHidden']) && empty($fields['renderFailed']) && is_string($fields['contentHtml'] ?? null)
                && self::str($item['relationships']['discussion']['data']['id'] ?? null) === self::str($data['id'] ?? null)) {
                $html = $fields['contentHtml'];
            }
        }

        return [
            'title' => trim(self::str($attributes['title'] ?? null)),
            'createdAt' => self::str($attributes['createdAt'] ?? null),
            'replies' => max(0, (int) ($attributes['commentCount'] ?? 0) - 1),
            'author' => $author,
            'html' => $html,
        ];
    }

    /** DiscussionForumPosting for a discussion() result whose first post is present. */
    public static function posting(array $discussion, string $url): array
    {
        $posting = ['@context' => 'https://schema.org', '@type' => 'DiscussionForumPosting', 'headline' => $discussion['title']];
        if ($url !== '') {
            $posting['url'] = $url;
        }
        try {
            if ($discussion['createdAt'] !== '') {
                $posting['datePublished'] = (new DateTimeImmutable($discussion['createdAt']))->format(DATE_ATOM);
            }
        } catch (Throwable) {
        }
        if ($discussion['author'] !== '') {
            $posting['author'] = ['@type' => 'Person', 'name' => $discussion['author']];
        }
        $posting['text'] = self::plain((string) $discussion['html']);
        $posting['interactionStatistic'] = [
            '@type' => 'InteractionCounter',
            'interactionType' => 'https://schema.org/CommentAction',
            'userInteractionCount' => $discussion['replies'],
        ];

        return $posting;
    }

    private static function str(mixed $value): string
    {
        return is_scalar($value) ? (string) $value : '';
    }
}

/** Head entry that renders the tags from the finished Document. */
final class SeededSeoHead
{
    public function __construct(
        private object $document,
        private bool $home,
        private string $title,
        private string $description
    ) {
    }

    public function __toString(): string
    {
        try {
            return $this->render();
        } catch (Throwable) {
            return $this->home ? '' : SeededSeo::meta('description', $this->description);
        }
    }

    private function render(): string
    {
        $document = $this->document;
        $url = is_string($document->canonicalUrl) ? $document->canonicalUrl : '';
        if ($url !== '' && method_exists($document, 'setPageParam')) {
            $url = (string) $document::setPageParam($url, $document->page);
        }
        if ($this->home) {
            return SeededSeo::tags('website', $this->title, $this->description, $url);
        }
        $api = json_decode((string) json_encode($document->payload['apiDocument'] ?? null, JSON_INVALID_UTF8_SUBSTITUTE), true);
        $discussion = SeededSeo::discussion(is_array($api) ? $api : []);
        $text = $discussion === null ? '' : SeededSeo::plain((string) $discussion['html']);
        $description = $text === '' ? $this->description : SeededSeo::excerpt($text);
        // Another extension may have set a description after attach(); keep one tag.
        $head = isset($document->meta['description']) ? [] : [SeededSeo::meta('description', $description)];
        if ($discussion !== null) {
            $head[] = SeededSeo::tags('article', $discussion['title'] ?: (string) $document->title, $description, $url);
        }
        if ($text !== '') {
            $head[] = SeededSeo::jsonLd(SeededSeo::posting($discussion, $url));
        }

        return implode("\n", $head);
    }
}
