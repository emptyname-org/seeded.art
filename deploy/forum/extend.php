<?php

// seeded.art forum extension: sign in with a seeded.art Tezos address (OAuth2),
// plus description, Open Graph and discussion structured data.
// Install: copy this file and seeded/ into the Flarum root, then clear the Flarum cache.

use Flarum\Extend;
use Flarum\Frontend\Document;
use Flarum\User\Event\Saving;
use Psr\Http\Message\ServerRequestInterface;
use Seeded\Forum\SeededAuthController;
use Seeded\Forum\SeededEmailGuard;
use Seeded\Forum\SeededSeo;

require_once __DIR__.'/seeded/SeededJson.php';
require_once __DIR__.'/seeded/SeededAuth.php';
require_once __DIR__.'/seeded/SeededSeo.php';

return [
    (new Extend\Routes('forum'))->get('/auth/seeded', 'auth.seeded', SeededAuthController::class),
    (new Extend\Event())->listen(Saving::class, SeededEmailGuard::class),
    (new Extend\Frontend('forum'))
        ->js(__DIR__.'/seeded/seeded-auth.js')
        ->content(function (Document $document, ServerRequestInterface $request) {
            SeededSeo::attach($document, $request->getUri()->getPath(), $request->getAttribute('routeName'));
        }),
];
