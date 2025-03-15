
# Copyright (c) 2021-2025, PostgreSQL Global Development Group

# Verify that work items work correctly in default environment

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

my $node = PostgreSQL::Test::Cluster->new('test');
$node->init;
$node->start;

my $result;

$node->safe_psql('postgres', 'create extension pg_index_stats');

$node->psql('postgres', "SELECT pg_index_stats_probe(-42)", stderr => \$result, on_error_stop => 1);
unlike(
	$result,
	qr/.+psql:<stdin>:1: ERROR:  Number of candidate queries is outside the valid range.+/,
	"Incorrect input");

$result = $node->safe_psql('postgres', "SELECT pg_index_stats_probe(1)");
is($result, 'f', "No pg_stat_statements installed");

#$node->append_conf('postgresql.conf', 'autovacuum_naptime=1s');


$node->stop;

done_testing();
