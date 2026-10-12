<?php

// JSON sign-in results for agents and other plain HTTP clients. No Flarum
// dependencies, so tests can load this file on its own.

namespace Seeded\Forum;

final class SeededJson
{
    /** Whether the client asks for JSON instead of the browser popup page. */
    public static function wanted(string $accept): bool
    {
        return str_contains(strtolower($accept), 'application/json');
    }

    /** The payload Flarum's popup page passes to app.authenticationComplete, or null. */
    public static function payload(string $html): ?array
    {
        if (! preg_match('/authenticationComplete\((\{.*\})\);<\/script>$/s', $html, $match)) {
            return null;
        }
        $payload = json_decode($match[1], true);

        return is_array($payload) ? $payload : null;
    }
}
