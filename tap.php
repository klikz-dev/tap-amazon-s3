#!/usr/bin/env php
<?php

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

define('TAP_BASE_PATH', dirname(__FILE__));

require_once(TAP_BASE_PATH . '/vendor/autoload.php');

(new AmazonS3Tap())->run();
