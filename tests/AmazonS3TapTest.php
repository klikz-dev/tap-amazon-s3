<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;

class AmazonS3TapTest extends TestCase
{
    public function testHasDesiredMethods()
    {
        $this->assertTrue(method_exists('AmazonS3Tap', 'test'));
        $this->assertTrue(method_exists('AmazonS3Tap', 'discover'));
        $this->assertTrue(method_exists('AmazonS3Tap', 'tap'));
        $this->assertTrue(method_exists('AmazonS3Tap', 'getTables'));
    }
}
