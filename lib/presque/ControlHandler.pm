package presque::ControlHandler;

use JSON;
use Moose;
extends 'Tatsumaki::Handler';

with
  'presque::Role::Queue::Names',
  'presque::Role::Error',
  'presque::Role::Response',
  'presque::Role::Queue::WithQueueName' => {methods => [qw/get post/]},;

__PACKAGE__->asynchronous(1);

sub get {
    my ($self, $queue_name) = @_;

    $self->application->redis->mget(
        $self->_queue_stat($queue_name),
        sub {
            my $res = shift;
            $self->entity(
                {   queue          => $queue_name,
                    status         => $res->[0],
                }
            );
        }
    );
}

sub post {
    my ( $self, $queue_name ) = @_;

    my $content = $self->request->content;

    return $self->http_error('content is missing') if !$content;

    my $json = JSON::decode_json( $content );
    if ( $json->{status} eq 'start' ) {
        $self->_set_status( $queue_name, 1 );
    }
    elsif ( $json->{status} eq 'stop' ) {
        $self->_set_status( $queue_name, 0 );
    }
    else {
        $self->http_error('invalid status '.$content->{status});
    }
}

sub _set_status {
    my ($self, $queue_name, $status) = @_;

    my $key = $self->_queue_stat($queue_name);

    $self->application->redis->set($key, $status);
    $self->entity(
        {   queue    => $queue_name,
            response => 'updated',
        }
    );
}

1;
__END__

=head1 NAME

presque::ControlHandler

=head1 SYNOPSIS

    # stop a queue
    curl -X POST -H 'Content-Type: application/json' -d '{"status":"stop"}' http://localhost:5000/control/:queue_name

    # start a queue
    curl -X POST -H 'Content-Type: application/json' -d '{"status":"start"}' http://localhost:5000/control/:queue_name

    # fetch the status of a queue
    curl http://localhost:5000/control/:queue_name

=head1 DESCRIPTION

By default, when a queue is created, the status is set to 'open'. When a queue is set to 'stop', no job will be fetched from the queue, but it's still possible to add new jobs.

=head1 METHODS

=head2 GET

=over 4

=item path

/control/:queue_name

=item request

=item response

content-type : application/json

code : 200

content : {"status":"0","queue":"foo"}

=back

If there is some delayed jobs in this queue, the date for the nearlier delayed job will be return in the response. The informations returned are:

=over 2

=item B<queue>

name of the queue

=item B<status>

status of the queue: 1 or 0

=back

=head2 POST

=over 4

=item path

/control/:queue_name

=item headers

content-type : application/json

=item request

content : {"status":"stop"}

=item response

content-type : application/json

content : {"response":"updated","queue":"foo"}

=back

Use this method to B<start> or B<stop> a queue.

=head1 AUTHOR

franck cuny E<lt>franck@lumberjaph.netE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright 2010 by Linkfluence

L<http://linkfluence.net>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
