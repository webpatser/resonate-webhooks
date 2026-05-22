<?php

namespace Webpatser\ResonateWebhooks\Tests\Support;

use Webpatser\Resonate\Application;
use Webpatser\Resonate\Contracts\Connection;

/**
 * A minimal in-memory Connection for driving the plugin in tests.
 */
class FakeConnection extends Connection
{
    /**
     * Create a new fake connection.
     */
    public function __construct(protected string $socketId, protected Application $appInstance)
    {
        $this->origin = 'http://localhost';
    }

    /**
     * Get the raw socket connection identifier.
     */
    public function identifier(): string
    {
        return $this->socketId;
    }

    /**
     * Get the normalized socket ID.
     */
    public function id(): string
    {
        return $this->socketId;
    }

    /**
     * Get the application the connection belongs to.
     */
    public function app(): Application
    {
        return $this->appInstance;
    }

    /**
     * Send a message to the connection.
     */
    public function send(string $message): void
    {
        //
    }

    /**
     * Send a control frame to the connection.
     */
    public function control(string $type = self::CONTROL_PING): void
    {
        //
    }

    /**
     * Terminate the connection.
     */
    public function terminate(): void
    {
        //
    }
}
