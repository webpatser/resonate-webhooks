<?php

namespace Webpatser\ResonateWebhooks\Tests;

use Illuminate\Support\ServiceProvider;
use Orchestra\Testbench\TestCase as Testbench;
use Webpatser\Resonate\ResonateServiceProvider;
use Webpatser\ResonateRoster\RosterServiceProvider;
use Webpatser\ResonateWebhooks\WebhooksServiceProvider;

class TestCase extends Testbench
{
    /**
     * Get the package providers.
     *
     * @return array<int, class-string<ServiceProvider>>
     */
    protected function getPackageProviders($app)
    {
        return [
            ResonateServiceProvider::class,
            RosterServiceProvider::class,
            WebhooksServiceProvider::class,
        ];
    }

    /**
     * Define the test environment.
     *
     * Mirrors Resonate's single-app `config/reverb.php`, and points both the
     * roster and the webhooks plugin at Redis database 15.
     */
    protected function defineEnvironment($app): void
    {
        $app['config']->set('reverb.default', 'reverb');

        $app['config']->set('reverb.servers.reverb', [
            'host' => '0.0.0.0',
            'port' => 8080,
            'path' => '',
            'hostname' => null,
            'options' => ['tls' => []],
            'max_request_size' => 10_000,
            'scaling' => [
                'enabled' => false,
                'channel' => 'reverb',
                'server' => [
                    'url' => null,
                    'host' => '127.0.0.1',
                    'port' => '6379',
                    'username' => null,
                    'password' => null,
                    'database' => '15',
                    'timeout' => 60,
                ],
            ],
            'pulse_ingest_interval' => 15,
            'telescope_ingest_interval' => 15,
        ]);

        $app['config']->set('reverb.apps', [
            'provider' => 'config',
            'apps' => [
                [
                    'key' => 'app-key',
                    'secret' => 'app-secret',
                    'app_id' => 'app-id',
                    'options' => [
                        'host' => 'localhost',
                        'port' => 8080,
                        'scheme' => 'http',
                        'useTLS' => false,
                    ],
                    'allowed_origins' => ['*'],
                    'ping_interval' => 60,
                    'activity_timeout' => 30,
                    'max_connections' => null,
                    'max_message_size' => 10_000,
                    'accept_client_events_from' => 'members',
                    'rate_limiting' => [
                        'enabled' => false,
                        'max_attempts' => 60,
                        'decay_seconds' => 60,
                        'terminate_on_limit' => false,
                    ],
                ],
            ],
        ]);

        $redis = [
            'url' => null,
            'host' => '127.0.0.1',
            'port' => '6379',
            'username' => null,
            'password' => null,
            'database' => '15',
            'timeout' => 5,
        ];

        $app['config']->set('resonate-roster', [
            'connection' => $redis,
            'key_prefix' => 'roster-test',
            'ttl' => 90,
            'heartbeat_interval' => 30,
            'track' => 'all',
        ]);

        $app['config']->set('resonate-webhooks', [
            'connection' => $redis,
            'key_prefix' => 'wh-test',
            'ttl' => 90,
            'flush_interval' => 1.0,
            'reconcile_interval' => 30.0,
            'max_attempts' => 5,
            'endpoints' => [
                [
                    'url' => 'http://localhost/webhook',
                    'app_id' => '*',
                    'events' => [
                        'channel_occupied', 'channel_vacated',
                        'member_added', 'member_removed', 'client_event',
                    ],
                ],
            ],
        ]);
    }
}
