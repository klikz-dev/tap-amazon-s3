# Creating a Tap
To get started with your own tap, you'll need to create a project based off of this repo with composer. You can do this with clone, but ideally, you'll use `create-project` via our Packagist repo:

`composer create-project datamanagementinc/singer-template --add-repository --repository='{"packagist.org": false}' --repository="https://repo.packagist.com/datamanagementinc/" tap-foobar`

Note: `tap-foobar` will be the name of the folder created for the project.

This will create the project and install all composer requirements.

# The Singer Spec in PHP
This is a Singer tap that produces JSON-formatted data following the Singer spec.

This tap:

Pulls raw data from a source, which is used to:
* Create a table based on a schema returned
* Incrementally pull data based on the input state

# The Test Method
Uses credential data from the user to test if data can be retrieved from the source

# The Discover Method
Retrieves schema data from this taps source

Then outputs data to standard output, which is used to build a table for this tap

# The Tap Method
Retrieves a group of records from the taps source for a schema based on a sent in configuration

Then outputs data to standard output, which can be used to add records to that table

# Running PHPStan

This repo comes with PHPStan as a composer dev requirement. PHPStan helps you find bugs in your code without running it. Super helpful.

It can be ran with: `./vendor/bin/phpstan analyse *Tap.php`

If you want it to be more strict, you can run it with a higher level (5 is the default): `./vendor/bin/phpstan analyse *Tap.php -l 9'
