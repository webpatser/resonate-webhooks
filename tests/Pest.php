<?php

use Revolt\EventLoop;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelConnectionManager;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelManager;
use Webpatser\Resonate\Protocols\Pusher\Managers\ArrayChannelConnectionManager;
use Webpatser\Resonate\Protocols\Pusher\Managers\ArrayChannelManager;
use Webpatser\ResonateWebhooks\Tests\TestCase;

uses(TestCase::class)->in(__DIR__.'/Feature', __DIR__.'/Integration');

/*
 * The Pusher channel managers are bound per test so the plugins and the
 * PluginContext exercise a real, isolated channel registry.
 */
uses()->beforeEach(function () {
    $this->app->singleton(ChannelManager::class, fn () => new ArrayChannelManager);
    $this->app->bind(ChannelConnectionManager::class, fn () => new ArrayChannelConnectionManager);
})->in(__DIR__.'/Integration');

/**
 * Determine whether a Redis server is reachable for the integration tests.
 */
function redisReachable(): bool
{
    $connection = @fsockopen('127.0.0.1', 6379, $errno, $errstr, 0.5);

    if ($connection === false) {
        return false;
    }

    fclose($connection);

    return true;
}

/**
 * Build a valid Pusher presence auth token for a connection on a channel.
 */
function presenceAuth(string $socketId, string $channel, string $data, string $secret = 'app-secret'): string
{
    return 'app-key:'.hash_hmac('sha256', "{$socketId}:{$channel}:{$data}", $secret);
}

/**
 * Run a closure inside the Revolt event loop, surfacing any failure.
 */
function runLoop(Closure $body): void
{
    $error = null;

    $watchdog = EventLoop::delay(5.0, function () use (&$error) {
        $error = 'event loop timed out';
        EventLoop::getDriver()->stop();
    });

    EventLoop::queue(function () use ($body, &$error, $watchdog) {
        try {
            $body();
        } catch (Throwable $e) {
            $error = $e->getMessage()."\n".$e->getTraceAsString();
        } finally {
            EventLoop::cancel($watchdog);
            EventLoop::getDriver()->stop();
        }
    });

    EventLoop::run();

    if ($error !== null) {
        throw new RuntimeException($error);
    }
}
