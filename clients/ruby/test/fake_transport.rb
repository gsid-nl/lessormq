# frozen_string_literal: true

# A scripted LessorMQ server for the unit tests: answers queued per path, every request
# recorded.
class FakeTransport
  Request = Struct.new(:verb, :path, :headers, :body, :timeout) do
    def json
      body ? JSON.parse(body) : {}
    end
  end

  attr_reader :requests

  def initialize
    @answers = Hash.new { |hash, path| hash[path] = [] }
    @requests = []
    @lock = Mutex.new
  end

  # Answers for a path, in order: [status, body] or [status, body, headers], or an
  # exception to raise. The last one repeats.
  def on(path, *answers)
    @answers[path].concat(answers)
    self
  end

  def calls(path)
    @lock.synchronize { @requests.select { |r| r.path == path }.map(&:json) }
  end

  def request(method, path, headers, body, timeout)
    answer = @lock.synchronize do
      @requests << Request.new(method, path, headers, body, timeout)
      queue = @answers[path]
      next [404, { "code" => "not_found", "detail" => "no answer scripted" }] if queue.empty?

      queue.size > 1 ? queue.shift : queue.first
    end
    raise answer if answer.is_a?(Exception)

    status, body, response_headers = answer
    [status, response_headers || {}, body.nil? ? nil : (body.is_a?(String) ? body : JSON.generate(body))]
  end
end
