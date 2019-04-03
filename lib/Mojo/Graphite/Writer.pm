package Mojo::Graphite::Writer;

use Mojo::Base -base;

use feature 'current_sub';

use Carp ();
use Mojo::IOLoop;
use Mojo::Promise;

use constant DEBUG => $ENV{MOJO_GRAPHITE_WRITER_DEBUG};

our $VERSION = '0.02';
$VERSION = eval $VERSION;

has address    => sub { Carp::croak 'address is required' };
has batch_size => 100;
has port       => 2003;

sub close {
  my $self = shift;
  my $stream = $self->{stream};
  $stream->close;
  return $self;
}

sub connect {
  my ($self, %args) = @_;
  my $p = Mojo::Promise->new;
  if (my $stream = $self->{stream}) {
    say STDERR "Reusing existing Graphite connection" if DEBUG;
    $p->resolve($stream);
  } else {
    $args{address} //= $self->address;
    $args{port} //= $self->port;
    say STDERR "Connecting to Graphite on $args{address}:$args{port}" if DEBUG;
    Mojo::IOLoop->client(%args, sub {
      my (undef, $err, $stream) = @_;
      if ($err) {
        say STDERR "Error opening Graphite socket: $err" if DEBUG;
        return $p->reject($err);
      }
      say STDERR "Graphite socket opened" if DEBUG;
      $stream->on(write => sub { say STDERR "Writing @{[length $_[1]]} bytes to Graphite" }) if DEBUG;
      $stream->on(close => sub {
        say STDERR "Graphite socket closed" if DEBUG;
        delete $self->{stream};
      });
      $self->{stream} = $stream;
      $p->resolve($stream);
    });
  }
  return $p;
}

sub write {
  my ($self, @metrics) = @_;
  my $p = Mojo::Promise->new;
  push @{ $self->{queue} }, [\@metrics, $p];
  $self->_write;
  return $p;
}

sub _write {
  my $self = shift;
  return unless @{ $self->{queue} ||= [] };

  return if $self->{writing};
  $self->{writing} = 1;

  $self->connect->then(
    sub {
      my $stream = shift;
      my $write = sub {
        my $queue = $self->{queue};

        # queue is empty
        unless (@$queue) {
          $self->{writing} = 0;
          return;
        }

        # this batch is done
        unless (@{ $queue->[0][0] }) {
          my $item = shift @$queue;
          my $p = $item->[1];
          $p->resolve;
          return unless @$queue;
        }

        my $string = join '', map { chomp; "$_\n" } splice @{ $queue->[0][0] }, 0, $self->batch_size;
        $stream->write($string, __SUB__);
      };

      $write->();
    },
    sub {
      my $err = shift;
      $_->[1]->reject($err) for @{ $self->{queue} };
      $self->{queue} = [];
      $self->{writing} = 0;
    }
  );
}

1;

=head1 NAME

Mojo::Graphite::Writer - A non-blocking Graphite metric writer using the Mojo stack

=head1 SYNOPSIS

  my $graphite = Mojo::Graphite::Writer->new(address => 'graphite.myhost.com');
  my $time = time;
  $graphite->write(
    "my.metric.one 1 $time",
    "my.metric.two 2 $time",
    ...
  );

=head1 DESCRIPTION

L<Mojo::Graphite::Writer> is a non-blocking client for feeding data to the Graphite metrics collector.
This simple module is meant to aid in batching and queuing writes to the server.

This is still a work-in-progress, however the author uses it in work application so every effort will be made to keep the api reasonably stable while improving where possible.

=head1 ATTRIBUTES

L<Mojo::Graphite::Writer> inherits all attributes from L<Mojo::Base> and implements the following new ones.

=head2 address

Address of the target Graphite server.
Required.

=head2 batch_size

The number of metrics to send in each write batch.
Default is 100.

=head2 port

Port of the target Graphite server.
Default is C<2003>.

=head1 METHODS

L<Mojo::Graphite::Writer> inherits all methods from L<Mojo::Base> and implements the following new ones.

=head2 close

Close the current connection to L</address>.

=head2 connect

Open a new connection to L</address>:L</port> using L<Mojo::IOLoop/client>.
Any additional arguments are passed through to that method.
Returns a L<Mojo::Promise> that resolves with the L<Mojo::IOLoop::Stream> object of the connection.

Note that if the client is already connected, the promise is resolved again with the same stream and will until that stream is closed.
In this way, for simple connections, you may simple call L</write> while for more complex ones, you may open the connction using this method with additional arguments if needed and then call L</write> later.

=head2 write

Write metrics to the L</connect>-ed graphite server.
Metrics are queued and written to the server in a non-blocking way, in the order that L</write> is called.

Metrics are strings of the form C<path value time> as documented as L<"the plaintext protocol"|https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol>.
Each string is one metric.
It will be line-ending normalized, no newline is required.
Writes are batched in groups of size L</batch_size>.

If the writer is not already connected, calling write will implicitly call L</connect>.

Returns a L<Mojo::Promise> that will be resolved when the metrics passed B<in this write call> are written.
The promise is rejected if any write in the write queue fails, even if it is not from the write call.

=head1 NOTE

This module is still in early development.
Future work will include

=over

=item *

Passing structures to L</write> and handling the formatting

=item *

Possibly a blocking api, though this is questionable

=item *

Testing

=back

=head1 SEE ALSO

=over

=item *

L<https://graphite.readthedocs.io/en/latest/>

=back

=head1 THANKS

This module's development was sponsored by L<ServerCentral Turing Group|https://www.servercentral.com/>.

=head1 SOURCE REPOSITORY

L<http://github.com/jberger/Mojo-Redfish-Client>

=head1 AUTHOR

Joel Berger, E<lt>joel.a.berger@gmail.comE<gt>

=head1 CONTRIBUTORS

None yet.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2019 by L</AUTHOR> and L</CONTRIBUTORS>

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.


