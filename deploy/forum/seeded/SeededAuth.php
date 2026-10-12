<?php

// Sign in with seeded.art: OAuth2 authorization code against the site's
// Tezos address sign-in. Address accounts get a reserved, undeliverable
// email (<address>@seeded.invalid) and never receive email notifications.
// With `Accept: application/json` the callback answers in JSON instead of the
// browser popup page, so agents can sign in with a plain HTTP client.

namespace Seeded\Forum;

use Flarum\Forum\Auth\Registration;
use Flarum\Forum\Auth\ResponseFactory;
use Flarum\Foundation\ValidationException;
use Flarum\Http\UrlGenerator;
use Flarum\Settings\SettingsRepositoryInterface;
use Flarum\User\Event\Saving;
use Flarum\User\LoginProvider;
use Flarum\User\RegistrationToken;
use Flarum\User\User;
use GuzzleHttp\Client;
use Illuminate\Support\Arr;
use Laminas\Diactoros\Response\HtmlResponse;
use Laminas\Diactoros\Response\JsonResponse;
use Laminas\Diactoros\Response\RedirectResponse;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\RequestHandlerInterface;
use Illuminate\Contracts\Cache\Repository as Cache;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;

final class SeededAuth
{
    public const PROVIDER = 'seeded';
    public const EMAIL_DOMAIN = 'seeded.invalid';

    public static function placeholder(?string $email): bool
    {
        return is_string($email) && str_ends_with(strtolower($email), '@'.self::EMAIL_DOMAIN);
    }
}

final class SeededAuthController implements RequestHandlerInterface
{
    private const STATE = 'seeded_oauth_state';
    private const ADDRESS = '/^tz[1-4][1-9A-HJ-NP-Za-km-z]{33}$/';
    private const USERNAME = '/^[A-Za-z0-9_-]{3,30}$/';
    // Callbacks per visitor address per minute; each one costs seeded.art a token exchange.
    private const CALLBACKS_PER_MINUTE = 10;

    public function __construct(
        private ResponseFactory $response,
        private SettingsRepositoryInterface $settings,
        private UrlGenerator $url,
        private LoggerInterface $log,
        private Cache $cache
    ) {
    }

    public function handle(ServerRequestInterface $request): ResponseInterface
    {
        $json = SeededJson::wanted($request->getHeaderLine('Accept'));
        $origin = rtrim((string) $this->settings->get('seeded-auth.origin'), '/');
        $client = (string) $this->settings->get('seeded-auth.client_id');
        $secret = (string) $this->settings->get('seeded-auth.client_secret');
        if ($origin === '' || $client === '' || $secret === '') {
            return $this->fail($json, 'Sign-in with seeded.art is not configured.');
        }
        $redirect = $this->url->to('forum')->route('auth.seeded');
        $session = $request->getAttribute('session');
        $query = $request->getQueryParams();
        $code = Arr::get($query, 'code');

        if (! is_string($code) || $code === '') {
            if (Arr::has($query, 'error')) {
                $session->remove(self::STATE);

                return $this->fail($json, 'Sign-in was cancelled or failed. Close this window and try again.');
            }
            $state = bin2hex(random_bytes(16));
            $session->put(self::STATE, $state);

            return new RedirectResponse($origin.'/oauth/authorize?'.http_build_query([
                'response_type' => 'code',
                'client_id' => $client,
                'redirect_uri' => $redirect,
                'state' => $state,
            ], '', '&', PHP_QUERY_RFC3986));
        }

        $key = 'seeded-auth-callbacks:'.sha1((string) $request->getAttribute('ipAddress'));
        $this->cache->add($key, 0, 60);
        if ($this->cache->increment($key) > self::CALLBACKS_PER_MINUTE) {
            return $this->fail($json, 'Too many sign-in attempts. Wait a minute and try again.');
        }

        $expected = $session->get(self::STATE);
        $session->remove(self::STATE);
        $state = Arr::get($query, 'state');
        if (! is_string($expected) || ! is_string($state) || ! hash_equals($expected, $state)) {
            return $this->fail($json, 'Sign-in expired. Close this window and try again.');
        }

        try {
            $http = new Client(['timeout' => 10, 'connect_timeout' => 5, 'allow_redirects' => false]);
            $token = json_decode((string) $http->post($origin.'/oauth/token', ['form_params' => [
                'grant_type' => 'authorization_code',
                'code' => $code,
                'redirect_uri' => $redirect,
                'client_id' => $client,
                'client_secret' => $secret,
            ]])->getBody(), true, 8, JSON_THROW_ON_ERROR);
            $access = is_array($token) ? Arr::get($token, 'access_token') : null;
            if (! is_string($access) || $access === '') {
                throw new RuntimeException('token response without access_token');
            }
            $owner = json_decode((string) $http->get($origin.'/oauth/user', ['headers' => [
                'Authorization' => 'Bearer '.$access,
                'Accept' => 'application/json',
            ]])->getBody(), true, 8, JSON_THROW_ON_ERROR);
        } catch (Throwable $e) {
            $this->log->warning('seeded.art sign-in failed: '.$e->getMessage());

            return $this->fail($json, 'seeded.art could not complete the sign-in. Close this window and try again.');
        }

        $address = is_array($owner) ? Arr::get($owner, 'id') : null;
        if (! is_string($address) || ! preg_match(self::ADDRESS, $address)) {
            return $this->fail($json, 'seeded.art returned an invalid address.');
        }
        $email = $address.'@'.SeededAuth::EMAIL_DOMAIN;
        // Flarum links a sign-in to any account holding the provided email; only ours may hold it.
        $linked = LoginProvider::where('provider', SeededAuth::PROVIDER)->where('identifier', $address)->exists();
        if (! $linked && User::where('email', $email)->exists()) {
            return $this->fail($json, 'This address cannot sign in. Contact the forum admin.');
        }
        $username = Arr::get($owner, 'username');

        $response = $this->response->make(SeededAuth::PROVIDER, $address, function (Registration $registration) use ($email, $username, $address) {
            $registration->provideTrustedEmail($email)->setPayload(['address' => $address]);
            if (is_string($username) && preg_match(self::USERNAME, $username)) {
                $registration->suggestUsername($username);
            }
        });
        $payload = $json ? SeededJson::payload((string) $response->getBody()) : null;
        if ($payload === null) {
            return $response;
        }
        // Same result as the popup: {"loggedIn":true} with the remember cookie, or a registration token.
        $result = new JsonResponse($payload);
        foreach ($response->getHeader('Set-Cookie') as $cookie) {
            $result = $result->withAddedHeader('Set-Cookie', $cookie);
        }

        return $result;
    }

    private function fail(bool $json, string $message): ResponseInterface
    {
        if ($json) {
            return new JsonResponse(['error' => $message], 400);
        }

        return new HtmlResponse('<!doctype html><meta charset="utf-8"><title>Sign-in</title><p>'
            .htmlspecialchars($message, ENT_QUOTES).'</p>', 400);
    }
}

final class SeededEmailGuard
{
    public function handle(Saving $event): void
    {
        $user = $event->user;
        if (! SeededAuth::placeholder($user->email)) {
            return;
        }
        if ($user->isDirty('email') && ! $this->registeringWithSeeded($event)) {
            throw new ValidationException(['email' => 'This email domain is reserved.']);
        }
        // The placeholder is undeliverable: keep every email notification off.
        foreach (array_keys($user->preferences) as $key) {
            if (str_starts_with($key, 'notify_') && str_ends_with($key, '_email')) {
                $user->setPreference($key, false);
            }
        }
    }

    private function registeringWithSeeded(Saving $event): bool
    {
        $token = Arr::get($event->data, 'attributes.token');
        if ($event->user->exists || ! is_string($token)) {
            return false;
        }
        $registration = RegistrationToken::find($token);

        return $registration !== null
            && $registration->provider === SeededAuth::PROVIDER
            && $event->user->email === $registration->identifier.'@'.SeededAuth::EMAIL_DOMAIN;
    }
}
