## kinesis-tail

Like running `tail -F` on a Kinesis stream!

This is very rough stage currently and many cases are not handled yet:
* Capturing new shards after a reshard event
* Dealing with closed shards
* Many other error cases

Currently it's useful to quickly run things like `tail | grep` patterns on active streams for debugging.

## Usage:

`kinesis-tail [stream-name]`

AWS Credentials should be configured under `~/.aws/credentials` in the same manner you would if using the `aws cli` tools.
For more information see: https://github.com/aws/aws-sdk-go#configuring-credentials
