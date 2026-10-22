# frozen_string_literal: true

module Worker
  module Jobs
    class ProcessQueuedMessagesJob < BaseJob

      # How many messages to try to lock from a priority level before looking again
      CANDIDATES_TO_LOCK = 10

      # How many times to look again before giving up because other workers took every candidate
      LOCK_ROUNDS = 3

      def call
        @lock_time = Time.current
        @locker = Postal.locker_name_with_suffix(SecureRandom.hex(8))

        find_ip_addresses
        lock_message_for_processing
        obtain_locked_messages
        process_messages
        @messages_to_process
      end

      private

      # Returns an array of IP address IDs that are present on the host that is
      # running this job.
      #
      # @return [Array<Integer>]
      def find_ip_addresses
        ip_addresses = { 4 => [], 6 => [] }
        Socket.ip_address_list.each do |address|
          next if local_ip?(address.ip_address)

          ip_addresses[address.ipv4? ? 4 : 6] << address.ip_address
        end
        @ip_addresses = IPAddress.where(ipv4: ip_addresses[4]).or(IPAddress.where(ipv6: ip_addresses[6])).pluck(:id)
      end

      # Is the given IP address a local address?
      #
      # @param [String] ip
      # @return [Boolean]
      def local_ip?(ip)
        !!(ip =~ /\A(127\.|fe80:|::)/)
      end

      # Lock the oldest available message at the highest priority level which has one, or a
      # random other from that level if a worker takes it first. Locked by ID because an UPDATE
      # scanning the priority index deadlocks with deletes. Gives up after LOCK_ROUNDS rather
      # than looping while other workers keep taking the candidates.
      #
      # @return [void]
      def lock_message_for_processing
        LOCK_ROUNDS.times do
          priority = top_lockable_priority
          return if priority.nil?

          oldest_id, *other_ids = lockable_messages.where(priority: priority).order(:id).limit(CANDIDATES_TO_LOCK).pluck(:id)
          next if oldest_id.nil?
          return if lock_message(oldest_id) || other_ids.shuffle.any? { |id| lock_message(id) }
        end
      end

      # The highest priority with a message this job can lock, ignoring any below this
      # worker's minimum. Read backwards along the priority index, stopping at the first match.
      #
      # @return [Integer, nil]
      def top_lockable_priority
        lockable_messages.where(priority: Postal::Config.worker.minimum_priority..)
                         .order(priority: :desc)
                         .pick(:priority)
      end

      # @return [ActiveRecord::Relation] queued messages which this job can lock
      def lockable_messages
        QueuedMessage.where(ip_address_id: [nil, @ip_addresses])
                     .where(locked_by: nil, locked_at: nil)
                     .ready_with_delayed_retry
      end

      # Lock a message unless another worker has locked it first
      #
      # @param [Integer] id
      # @return [Boolean]
      def lock_message(id)
        lockable_messages.where(id: id).update_all(locked_by: @locker, locked_at: @lock_time).positive?
      end

      # Get a full list of all messages which we can process (i.e. those which have just
      # been locked by us for processing)
      #
      # @return [void]
      def obtain_locked_messages
        @messages_to_process = QueuedMessage.where(locked_by: @locker, locked_at: @lock_time)
      end

      # Process the messages we obtained from the database
      #
      # @return [void]
      def process_messages
        @messages_to_process.each do |message|
          work_completed!
          MessageDequeuer.process(message, logger: logger)
        end
      end

    end
  end
end
