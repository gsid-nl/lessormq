# frozen_string_literal: true

require "net/http"
require "openssl"
require "uri"

module LessorMQ
  # Sends one HTTP request. The client brings everything else (the key, retries, errors),
  # so a transport of your own only has to answer #request:
  #
  #   request(method, path, headers, body, timeout) => [status, headers, body]
  #
  # +headers+ in the answer have lower-case names; +body+ is a String or nil. A request
  # that did not get an answer raises TransportError.
  #
  # This one is Net::HTTP with a small pool of kept-alive connections, so a worker's
  # claims and the extends from its keepalive thread can run at the same time.
  class NetHTTPTransport
    NETWORK_ERRORS = [
      IOError, SystemCallError, SocketError, Timeout::Error, Net::HTTPBadResponse, Net::ProtocolError,
      OpenSSL::SSL::SSLError
    ].freeze

    METHODS = { "GET" => Net::HTTP::Get, "POST" => Net::HTTP::Post, "PATCH" => Net::HTTP::Patch }.freeze

    # +open_timeout+ is the time to connect; the time to answer is per request.
    def initialize(base_url, open_timeout: 5, pool_size: 8)
      @uri = URI.parse(base_url)
      @open_timeout = open_timeout
      @pool_size = pool_size
      @idle = []
      @lock = Mutex.new
      @pid = Process.pid
    end

    def request(method, path, headers, body, timeout)
      http = checkout
      begin
        request = METHODS.fetch(method).new(@uri.path.chomp("/") + path, headers)
        request.body = body if body
        http.read_timeout = timeout
        http.write_timeout = timeout
        response = http.request(request)
      rescue *NETWORK_ERRORS => e
        http.finish if http.started?
        raise TransportError, "#{e.class}: #{e.message}"
      end
      checkin(http)
      [response.code.to_i, response.to_hash.transform_values { |values| values.join(", ") }, response.body]
    end

    private

    def checkout
      @lock.synchronize do
        # Connections from before a fork belong to the parent.
        if @pid != Process.pid
          @idle.clear
          @pid = Process.pid
        end
        @idle.pop
      end || connect
    end

    def checkin(http)
      @lock.synchronize do
        if @idle.size < @pool_size && @pid == Process.pid
          @idle.push(http)
          http = nil
        end
      end
      http&.finish if http&.started?
    end

    def connect
      http = Net::HTTP.new(@uri.host, @uri.port)
      http.use_ssl = @uri.scheme == "https"
      http.open_timeout = @open_timeout
      # Net::HTTP retries a request on its own only for idempotent methods; the client
      # decides about retries.
      http.max_retries = 0
      http.start
      http
    rescue *NETWORK_ERRORS => e
      raise TransportError, "#{e.class}: #{e.message}"
    end
  end
end
