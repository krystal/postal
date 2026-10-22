#!/usr/bin/env ruby
# frozen_string_literal: true

# Runs ProcessQueuedMessagesJob from several threads against seeded queued messages
# (priorities 100/50/0) and checks each is claimed once, in priority order, without
# deadlocks. Sending is replaced by batching and deleting, as a successful send does.
# It deletes queued messages, so only runs against a database named *test*.
#
#   POSTAL_CONFIG_FILE_PATH=config/postal/postal.test.yml RAILS_ENV=test \
#     bundle exec ruby script/queue_claim_concurrency_check.rb [ours|level-update|upstream|pr3446|all]
#
# level-update is one UPDATE ... ORDER BY id LIMIT 1 per priority level; upstream is the
# claim before priorities; pr3446 is postalserver/postal#3446's claim.
# Options: THREADS (8), MESSAGES (5000), DOMAINS (25), BATCH=0 to turn off batching.

$stdout.sync = true

require_relative "../config/environment"

MODE = ARGV[0] || "all"
THREADS = (ENV["THREADS"] || 8).to_i
MESSAGES = (ENV["MESSAGES"] || 5000).to_i
DOMAINS = (ENV["DOMAINS"] || 25).to_i
BATCH = ENV["BATCH"] != "0"
PRIORITIES = [100, 50, 0].freeze
DECOYS_PER_KIND = 30
MAX_RUN_TIME = 300
PERMALINK = "queue-claim-check"

# The batch lock as a single UPDATE, as upstream does it
module SingleUpdateBatch

  def lock_batch(message, with_priority:)
    return [] if message.batch_key.nil?

    time = Time.now
    locker = Postal.locker_name
    scope = QueuedMessage.ready.where(batch_key: message.batch_key, ip_address_id: message.ip_address_id, locked_by: nil, locked_at: nil)
    scope = scope.where(priority: message.priority) if with_priority
    scope.limit(100).update_all(locked_by: locker, locked_at: time)
    QueuedMessage.where(batch_key: message.batch_key, ip_address_id: message.ip_address_id, locked_by: locker, locked_at: time).where.not(id: message.id).to_a
  end

end

class OursJob < Worker::Jobs::ProcessQueuedMessagesJob

  def self.batch(message)
    message.batchable_messages(100).to_a
  end

end

# One UPDATE ... ORDER BY id LIMIT 1 per priority level.
class LevelUpdateJob < Worker::Jobs::ProcessQueuedMessagesJob

  extend SingleUpdateBatch

  def self.batch(message)
    lock_batch(message, with_priority: true)
  end

  private

  def lock_message_for_processing
    priority_levels.each do |priority|
      locked = QueuedMessage.where(ip_address_id: [nil, @ip_addresses])
                            .where(locked_by: nil, locked_at: nil, priority: priority)
                            .ready_with_delayed_retry
                            .order(:id)
                            .limit(1)
                            .update_all(locked_by: @locker, locked_at: @lock_time)
      break if locked.positive?
    end
  end

  def priority_levels
    QueuedMessage.distinct.order(:priority).pluck(:priority).reverse
  end

end

# The claim before priorities were added.
class UpstreamJob < Worker::Jobs::ProcessQueuedMessagesJob

  extend SingleUpdateBatch

  def self.batch(message)
    lock_batch(message, with_priority: false)
  end

  private

  def lock_message_for_processing
    QueuedMessage.where(ip_address_id: [nil, @ip_addresses])
                 .where(locked_by: nil, locked_at: nil)
                 .ready_with_delayed_retry
                 .limit(1)
                 .update_all(locked_by: @locker, locked_at: @lock_time)
  end

end

# The claim from postalserver/postal#3446.
class PR3446Job < Worker::Jobs::ProcessQueuedMessagesJob

  extend SingleUpdateBatch

  def self.batch(message)
    lock_batch(message, with_priority: false)
  end

  private

  def lock_message_for_processing
    QueuedMessage.joins(:server)
                 .where(ip_address_id: [nil, @ip_addresses])
                 .where(locked_by: nil, locked_at: nil)
                 .ready_with_delayed_retry
                 .order("servers.priority DESC, queued_messages.id ASC")
                 .limit(1)
                 .update_all(locked_by: @locker, locked_at: @lock_time)
  end

end

JOB_CLASSES = {
  "ours" => OursJob,
  "level-update" => LevelUpdateJob,
  "upstream" => UpstreamJob,
  "pr3446" => PR3446Job
}.freeze

unless MODE == "all" || JOB_CLASSES.key?(MODE)
  abort "usage: #{__FILE__} [#{JOB_CLASSES.keys.join('|')}|all]"
end

database = ActiveRecord::Base.connection.current_database
unless database.include?("test")
  abort "Refusing to run against '#{database}': this script deletes queued messages, so it only runs against a test database"
end

# Counts rows each thread's UPDATEs lock, to compare with the messages it processes
module LockCounter

  def update_all(updates)
    result = super
    if klass == QueuedMessage && updates.is_a?(Hash) && updates.key?(:locked_by)
      Thread.current[:locked_rows] = (Thread.current[:locked_rows] || 0) + result
    end
    result
  end

end
ActiveRecord::Relation.prepend(LockCounter)

# Replaces sending with a recorder
RECORDS = Concurrent::Array.new
RECORDS_MUTEX = Mutex.new
MessageDequeuer.define_singleton_method(:process) do |message, **|
  batch = BATCH ? Thread.current[:job_class].batch(message) : []
  RECORDS_MUTEX.synchronize do
    RECORDS << { seq: RECORDS.size, id: message.id, priority: message.priority, head: true, thread: Thread.current[:index] }
    batch.each do |other|
      RECORDS << { seq: RECORDS.size, id: other.id, priority: other.priority, head: false, head_priority: message.priority, thread: Thread.current[:index] }
    end
  end
  Thread.current[:processed] += 1 + batch.size
  ([message] + batch).each { |m| QueuedMessage.where(id: m.id).delete_all }
end

def statement_kind(sql)
  if sql.nil?
    :unknown
  elsif sql.start_with?("UPDATE `queued_messages`") && sql.include?("`locked_by` = ")
    sql.include?("`batch_key` = ") || sql.include?("`queued_messages`.`id` IN (") ? :batch_lock : :claim
  elsif sql.start_with?("SELECT DISTINCT `queued_messages`.`priority`")
    :priority_levels
  elsif sql.start_with?("SELECT `queued_messages`.`priority` FROM")
    :top_priority
  elsif sql.start_with?("SELECT `queued_messages`.`id` FROM `queued_messages`")
    sql.include?("`batch_key` = ") ? :batch_candidates : :candidates
  elsif sql.start_with?("DELETE FROM `queued_messages`")
    :delete
  elsif sql.start_with?("SELECT `queued_messages`")
    :select
  else
    :other
  end
end

# Counts the statements each mode sends
STATEMENTS = Hash.new(0)
STATEMENTS_MUTEX = Mutex.new
ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
  kind = statement_kind(payload[:sql])
  STATEMENTS_MUTEX.synchronize { STATEMENTS[kind] += 1 }
end

def clean_up
  organization = Organization.find_by(permalink: PERMALINK)
  return if organization.nil?

  server_ids = Server.where(organization_id: organization.id).pluck(:id)
  QueuedMessage.where(server_id: server_ids).delete_all
  Server.where(id: server_ids).delete_all
  owner_id = organization.owner_id
  Organization.where(id: organization.id).delete_all
  User.where(id: owner_id).delete_all
end

def seed
  user = User.create!(first_name: "Queue", last_name: "Check", email_address: "#{PERMALINK}@example.com", password: SecureRandom.hex(16))
  organization = Organization.create!(name: "Queue claim check", permalink: PERMALINK, owner: user)
  servers = PRIORITIES.map do |priority|
    Server.create!(organization: organization, name: "Priority #{priority}", mode: "Live", priority: priority, provision_database: false)
  end

  now = Time.current
  row = lambda do |server, index, extra = {}|
    domain = "domain#{index % DOMAINS}.example"
    { server_id: server.id, message_id: index, domain: domain, batch_key: "outgoing-#{domain}",
      priority: server.priority, attempts: 0, locked_by: nil, locked_at: nil, retry_after: nil,
      created_at: now, updated_at: now }.merge(extra)
  end

  # Messages which must never be claimed go first, so every claim skips them
  decoys = servers.flat_map do |server|
    Array.new(DECOYS_PER_KIND) { |i| row.call(server, i, locked_by: "another-worker", locked_at: now) } +
      Array.new(DECOYS_PER_KIND) { |i| row.call(server, i, attempts: 1, retry_after: 1.hour.from_now) }
  end
  QueuedMessage.insert_all!(decoys)

  # Interleave the servers' messages so that id order is not priority order
  messages = Array.new(MESSAGES) { |i| row.call(servers[i % servers.size], i) }.shuffle(random: Random.new(1))
  messages.each_slice(1000) { |slice| QueuedMessage.insert_all!(slice) }

  ActiveRecord::Base.connection.execute("ANALYZE TABLE queued_messages")
  claimable = QueuedMessage.where(server_id: servers.map(&:id), locked_by: nil, retry_after: nil)
  [servers, claimable.pluck(:id, :priority).to_h]
end

# Captures the SQL for one claim and batch without keeping their effects
def capture_claim_sql(job_class)
  statements = []
  subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
    statements << payload[:sql] if statement_kind(payload[:sql]) != :other
  end
  ActiveRecord::Base.transaction do
    job = job_class.new(logger: Postal.logger)
    job.instance_variable_set(:@lock_time, Time.current)
    job.instance_variable_set(:@locker, "capture")
    job.send(:find_ip_addresses)
    job.send(:lock_message_for_processing)
    message = QueuedMessage.find_by(locked_by: "capture")
    job_class.batch(message) if message
    raise ActiveRecord::Rollback
  end
  statements
ensure
  ActiveSupport::Notifications.unsubscribe(subscriber)
end

def explain(label, sql)
  puts "  EXPLAIN #{label}:"
  puts "    #{sql}"
  ActiveRecord::Base.connection.select_all("EXPLAIN #{sql}").each do |row|
    details = row.slice("select_type", "table", "type", "possible_keys", "key", "rows", "Extra").map { |k, v| "#{k}=#{v.inspect}" }
    puts "    -> #{details.join(' ')}"
  end
end

def print_explains(mode, statements)
  puts "Query plans (#{mode}):"
  statements.uniq { |sql| statement_kind(sql) }.each do |sql|
    next if statement_kind(sql) == :select

    explain(statement_kind(sql).to_s, sql)
  end
end

def innodb_status
  rows = ActiveRecord::Base.connection.select_rows("SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_deadlocks', 'Innodb_row_lock_waits', 'Innodb_row_lock_time')")
  rows.to_h.transform_values(&:to_i)
end

# The statements and locks in the latest deadlock, and which InnoDB rolled back
def latest_deadlock
  status = ActiveRecord::Base.connection.select_one("SHOW ENGINE INNODB STATUS")["Status"]
  section = status[/LATEST DETECTED DEADLOCK\n-+\n(.*?)\n-+\nTRANSACTIONS/m, 1]
  return [] if section.nil?

  section.lines.grep(/\A(UPDATE|DELETE|SELECT|INSERT|RECORD LOCKS|\*\*\* )/).map { |line| line.strip[0, 160] }
end

def run_threads(job_class, claimable_scope)
  errors = Hash.new(0)
  stolen = 0
  mutex = Mutex.new
  deadline = Time.now + MAX_RUN_TIME

  threads = Array.new(THREADS) do |index|
    Thread.new do
      Thread.current[:index] = index
      Thread.current[:job_class] = job_class
      ActiveRecord::Base.connection_pool.with_connection do
        loop do
          raise "Timed out after #{MAX_RUN_TIME} seconds" if Time.now > deadline

          Thread.current[:locked_rows] = 0
          Thread.current[:processed] = 0
          begin
            job_class.new(logger: Postal.logger).call
          rescue ActiveRecord::StatementInvalid => e
            mutex.synchronize { errors["#{e.class.name} in #{statement_kind(e.sql)}"] += 1 }
            next
          end

          if Thread.current[:locked_rows] > Thread.current[:processed]
            mutex.synchronize { stolen += Thread.current[:locked_rows] - Thread.current[:processed] }
          end

          break if Thread.current[:processed].zero? && !claimable_scope.exists?
        end
      end
    end
  end
  threads.each(&:join)
  [errors, stolen]
end

def check(mode)
  job_class = JOB_CLASSES.fetch(mode)
  puts
  puts "=" * 100
  puts "Mode: #{mode}, #{THREADS} threads, #{MESSAGES} messages to #{DOMAINS} domains over priorities #{PRIORITIES.join('/')}, batching #{BATCH ? 'on' : 'off'}"
  puts "=" * 100

  clean_up
  if QueuedMessage.exists?
    abort "queued_messages in '#{ActiveRecord::Base.connection.current_database}' has rows that this script did not create; empty it first"
  end

  servers, expected = seed
  claimable_scope = QueuedMessage.where(server_id: servers.map(&:id), locked_by: nil, locked_at: nil).ready_with_delayed_retry

  statements = capture_claim_sql(job_class)
  puts "SQL for one claim#{BATCH ? ' and batch' : ''} (#{mode}):"
  statements.each { |sql| puts "  #{sql}" }
  puts
  print_explains(mode, statements)
  puts

  RECORDS.clear
  STATEMENTS.clear
  status_before = innodb_status
  started_at = Time.now
  errors, stolen = run_threads(job_class, claimable_scope)
  elapsed = Time.now - started_at
  status_after = innodb_status

  processed_ids = RECORDS.map { |r| r[:id] }
  duplicates = processed_ids.tally.select { |_, count| count > 1 }
  missing = expected.keys - processed_ids
  unexpected = processed_ids.uniq - expected.keys
  heads = RECORDS.select { |r| r[:head] }
  mixed_batches = RECORDS.count { |r| !r[:head] && r[:priority] != r[:head_priority] }

  # A lower priority claim before the last higher priority claim (batched messages not counted)
  inversions = PRIORITIES.each_cons(2).to_h do |higher, lower|
    last_higher = heads.select { |r| r[:priority] >= higher }.map { |r| r[:seq] }.max || -1
    ["#{lower} before last #{higher}", heads.count { |r| r[:priority] <= lower && r[:seq] < last_higher }]
  end

  puts "Results (#{mode}):"
  puts "  elapsed:                  #{elapsed.round(2)}s (#{(RECORDS.size / elapsed).round} messages/s, #{(heads.size / elapsed).round} claims/s)"
  puts "  processed:                #{RECORDS.size} of #{expected.size} (#{heads.size} claims, #{RECORDS.size - heads.size} batched with them)"
  puts "  processed more than once: #{duplicates.size}#{duplicates.empty? ? '' : " e.g. #{duplicates.first(5).to_h}"}"
  puts "  never processed:          #{missing.size}"
  puts "  locked/retrying taken:    #{unexpected.size}"
  puts "  locks taken over:         #{stolen} (an UPDATE reported a row locked, but the thread then did not find it locked by itself)"
  puts "  errors (by statement):    #{errors.empty? ? 'none' : errors}"
  puts "  statements sent:          #{STATEMENTS.except(:other, :unknown)}"
  puts "  priority inversions:      #{inversions}"
  puts "  batched at another level: #{mixed_batches}"
  puts "  per thread processed:     #{RECORDS.group_by { |r| r[:thread] }.transform_values(&:size).sort.to_h}"
  deltas = status_after.to_h { |k, v| [k, v - status_before[k]] }
  puts "  InnoDB (server-wide):     #{deltas}"
  if deltas["Innodb_deadlocks"].positive?
    puts "  latest deadlock:"
    latest_deadlock.each { |line| puts "    #{line}" }
  end

  failures = []
  failures << "messages processed more than once" if duplicates.any?
  failures << "messages never processed" if missing.any?
  failures << "locked or retrying messages taken" if unexpected.any?
  failures << "errors" if errors.any?
  failures << "locks taken over" if stolen.positive?
  if %w[ours level-update].include?(mode)
    failures << "priority inversions" if inversions.values.any? { |count| count > THREADS }
    failures << "batches mixing priorities" if mixed_batches.positive?
  end
  puts "  verdict:                  #{failures.empty? ? 'PASS' : "FAIL (#{failures.join(', ')})"}"
  failures.empty?
ensure
  clean_up
end

Postal.change_database_connection_pool_size(THREADS + 3)
ActiveRecord::Base.connection.select_one("SELECT @@version AS version, @@tx_isolation AS isolation").then do |row|
  puts "Database: #{ActiveRecord::Base.connection.current_database} on #{row['version']}, #{row['isolation']}"
end

modes = MODE == "all" ? JOB_CLASSES.keys : [MODE]
results = modes.to_h { |mode| [mode, check(mode)] }
puts
puts "Summary: #{results.map { |mode, passed| "#{mode} #{passed ? 'PASS' : 'FAIL'}" }.join(', ')}"
exit(results.values.all? ? 0 : 1)
