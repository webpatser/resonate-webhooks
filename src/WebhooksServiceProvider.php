<?php

namespace Webpatser\ResonateWebhooks;

use Illuminate\Support\ServiceProvider;

/**
 * Wires the webhooks plugin into a host Laravel application.
 *
 * It merges the config and binds the {@see WebhookTransport} port to its HTTP
 * implementation. The {@see WebhookPlugin} itself is not bound here: Resonate
 * instantiates it from the `plugins` array in `config/reverb.php`.
 */
class WebhooksServiceProvider extends ServiceProvider
{
    /**
     * Register the package services.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/resonate-webhooks.php', 'resonate-webhooks');

        $this->app->bind(WebhookTransport::class, HttpWebhookTransport::class);
    }

    /**
     * Bootstrap the package services.
     */
    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__.'/../config/resonate-webhooks.php' => $this->app->configPath('resonate-webhooks.php'),
            ], 'resonate-webhooks-config');
        }
    }
}
