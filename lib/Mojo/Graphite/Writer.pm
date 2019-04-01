package Mojo::Graphite::Writer;

use Mojo::Base -base;

use feature 'current_sub';

use Carp ();
use Mojo::IOLoop;
use Mojo::Promise;

use constant DEBUG => $ENV{MOJO_GRAPHITE_WRITER_DEBUG};

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
  my $self = @_;
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

Mojo::Graphite::Writer - 

=head1 SEE ALSO

=over

=item L<https://redfish.dmtf.org>.

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


