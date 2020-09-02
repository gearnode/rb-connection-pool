# Copyright (c) 2020 Bryan Frimin <bryan@frimin.fr>.
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

module RbConnectionPool
  class ConnectionPool

    class Error < ::RuntimeError; end
    class PoolShuttingDownError < Error; end
    class TimeoutError < ::Timeout::Error; end

    class Pool
      def initialize(size = 0, &block)
        @q = []

        @mut = Mutex.new
        @cond = ConditionVariable.new

        @created = 0
        @max = size

        @create_block = block
        @shutdown_block = nil
      end

      # Release a connection in the pool.
      #
      # @author Gearnode <bryan@frimin.fr>
      # @since 1.0.0
      #
      # @param [Object] obj
      #
      # @return [void]
      def release(obj)
        @mut.synchronize do
          if @shutdown_block
            @shutdown_block.call(obj)
          else
            @q.push(obj)
          end

          @cond.broadcast
        end
      end

      # Acquire a connection from the pool.
      #
      # @author Gearnode <bryan@frimin.fr>
      # @since 1.0.0
      #
      # @param [Numeric] timeout
      #
      # @return [Object]
      def acquire(timeout = 0.5)
        deadline = current_time + timeout

        @mut.synchronize do
          loop do
            raise(PoolShuttingDownError, "cannot get connection: pool shutting down") if @shutdown_block
            return @q.pop() if !@q.empty?()

            conn = try_create()
            return conn if conn

            wait = deadline - current_time
            raise(TimeoutError, "cannot get connection: timeout after waited #{timeout} seconds") if wait <= 0
            @cond.wait(@mut, wait)
          end
        end
      end

      # Hook to close all connection in the pool.
      #
      # @author Gearnode <bryan@frimin.fr>
      # @since 1.0.0
      #
      # @yield [Object] the logic to close a connection
      #
      # @return [void]
      def shutdown(&block)
        raise(ArgumentError, "cannot shutdown pool: missing block") unless block_given?

        @mut.synchronize do
          @shutdown_block = block
          @cond.broadcast

          while !@q.empty?
            conn = @q.pop()
            @shutdown_block.call(conn)
          end
        end

        nil
      end

      # Returns the maxium size of the pool.
      #
      # @author Gearnode <bryan@frimin.fr>
      # @since 1.0.0
      #
      # @return [Numeric]
      def size
        @max
      end

      private

      # This method must be call in mutex context
      def try_create
        return if @created == @max

        obj = @create_block.call()
        @created += 1
        obj
      end

      def current_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end

    # Create a new connection pool
    #
    # @author Gearnode <bryan@frimin.fr>
    # @since 1.0.0
    #
    # @param size [Integer] the size of the pool.
    # @param timeout [Numeric]
    # @yield the logic to open a connection
    #
    # @return [ConnectionPool]
    def initialize(size: 10, timeout: 2.5, &block)
      raise(ArgumentError, "cannot create connection pool: missing block") unless block_given?

      @size = size
      @timeout = timeout
      @pool = Pool.new(@size, &block)
    end

    # Returns the maximum number of connections that can be open at any time.
    #
    # @author Gearnode <bryan@frimin.fr>
    # @since 1.0.0
    #
    # @return [Integer]
    def size
      @pool.size
    end

    # Returns an available connection taken from the queue
    #
    # 1. Pop a connection from the queue
    # 2. Try to create connection if no connections are available in the pool
    # 3. Wait new connection when it's not possible to create a new connection and
    #    no other connections are available in the queue
    #
    # @author Gearnode <bryan@frimin.fr>
    # @since 1.0.0
    #
    # @return [Object]
    def with(opts = {})
      timeout = opts.fetch(:timeout, @timeout)

      Thread.handle_interrupt(Exception => :never) do
        conn = @pool.acquire(timeout)

        begin
          Thread.handle_interrupt(Exception => :immediate) do
            yield conn
          end
        ensure
          @pool.release(conn)
        end
      end
    end

    # Shutdown all the connection in the queue with the given block.
    #
    # @author Gearnode <bryan@frimin.fr>
    # @since 1.0.0
    #
    # @yield [Object] connection in the queue
    #
    # @return [nil]
    def shutdown(&block)
      @pool.shutdown(&block)
      nil
    end
  end
end
