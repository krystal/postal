# frozen_string_literal: true

require "rails_helper"

module Worker
  module Jobs

    RSpec.describe ProcessQueuedMessagesJob do
      subject(:job) { described_class.new(logger: Postal.logger) }

      before do
        allow(MessageDequeuer).to receive(:process)
      end

      describe "#call" do
        context "when there are no queued messages" do
          it "does nothing" do
            job.call
            expect(MessageDequeuer).to_not have_received(:process)
          end
        end

        context "when there is an unlocked queued message for an IP address that is not ours" do
          it "does nothing" do
            ip_address = create(:ip_address)
            queued_message = create(:queued_message, ip_address: ip_address)
            job.call
            expect(MessageDequeuer).to_not have_received(:process)
            expect(queued_message.reload.locked?).to be false
          end
        end

        context "when there is an unlocked queued message without an IP address without a retry time" do
          it "locks the message and calls the service" do
            queued_message = create(:queued_message, ip_address: nil, retry_after: nil)
            job.call
            expect(MessageDequeuer).to have_received(:process).with(queued_message, logger: kind_of(Klogger::Logger))
            expect(queued_message.reload.locked?).to be true
            expect(queued_message.locked_by).to match(/\A#{Postal.locker_name} [a-f0-9]{16}\z/)
            expect(queued_message.locked_at).to be_within(1.second).of(Time.current)
          end
        end

        context "when there is an unlocked queued message without an IP address without a retry time in the past" do
          it "locks the message and calls the service" do
            queued_message = create(:queued_message, ip_address: nil, retry_after: 10.minutes.ago)
            job.call
            expect(MessageDequeuer).to have_received(:process).with(queued_message, logger: kind_of(Klogger::Logger))
            expect(queued_message.reload.locked?).to be true
            expect(queued_message.locked_by).to match(/\A#{Postal.locker_name} [a-f0-9]{16}\z/)
            expect(queued_message.locked_at).to be_within(1.second).of(Time.current)
          end
        end

        context "when there is an unlocked queued message without an IP address without a retry time in the future" do
          it "does nothing" do
            queued_message = create(:queued_message, ip_address: nil, retry_after: 10.minutes.from_now)
            job.call
            expect(MessageDequeuer).to_not have_received(:process)
            expect(queued_message.reload.locked?).to be false
          end
        end

        context "when there is a locked queued message without an IP address without a retry time" do
          it "does nothing" do
            queued_message = create(:queued_message, :locked, ip_address: nil, retry_after: nil)
            job.call
            expect(MessageDequeuer).to_not have_received(:process)
            expect(queued_message.reload.locked?).to be true
          end
        end

        context "when there is a locked queued message without an IP address with a retry time in the past" do
          it "does nothing" do
            queued_message = create(:queued_message, :locked, ip_address: nil, retry_after: 1.month.ago)
            job.call
            expect(MessageDequeuer).to_not have_received(:process)
            expect(queued_message.reload.locked?).to be true
          end
        end

        context "when there is an unlocked queued message with an IP address that is ours without a retry time" do
          it "locks the message and calls the service" do
            ip_address = create(:ip_address, ipv4: "10.20.30.40")
            allow(Socket).to receive(:ip_address_list).and_return([Addrinfo.new(["AF_INET", 1, "localhost.localdomain", "10.20.30.40"])])
            queued_message = create(:queued_message, ip_address: ip_address)
            job.call
            expect(MessageDequeuer).to have_received(:process).with(queued_message, logger: kind_of(Klogger::Logger))
            expect(queued_message.reload.locked?).to be true
            expect(queued_message.locked_by).to match(/\A#{Postal.locker_name} [a-f0-9]{16}\z/)
            expect(queued_message.locked_at).to be_within(1.second).of(Time.current)
          end
        end

        context "when there is an unlocked queued message with an IP address that is ours without a retry time in the future" do
          it "does nothing" do
            ip_address = create(:ip_address, ipv4: "10.20.30.40")
            allow(Socket).to receive(:ip_address_list).and_return([Addrinfo.new(["AF_INET", 1, "localhost.localdomain", "10.20.30.40"])])
            queued_message = create(:queued_message, ip_address: ip_address, retry_after: 1.month.from_now)
            job.call
            expect(MessageDequeuer).to_not have_received(:process)
            expect(queued_message.reload.locked?).to be false
          end
        end

        context "when there are queued messages from servers with different priorities" do
          let(:high_priority_server) { create(:server, priority: 100) }
          let(:low_priority_server) { create(:server, priority: 0) }

          it "locks a message from the highest priority server first" do
            low_priority_message = create(:queued_message, server: low_priority_server)
            high_priority_message = create(:queued_message, server: high_priority_server)
            job.call
            expect(MessageDequeuer).to have_received(:process).once
            expect(MessageDequeuer).to have_received(:process).with(high_priority_message, logger: kind_of(Klogger::Logger))
            expect(low_priority_message.reload.locked?).to be false
          end

          it "locks the oldest message within a priority level first" do
            oldest_message = create(:queued_message, server: high_priority_server)
            newest_message = create(:queued_message, server: high_priority_server)
            job.call
            expect(MessageDequeuer).to have_received(:process).once
            expect(MessageDequeuer).to have_received(:process).with(oldest_message, logger: kind_of(Klogger::Logger))
            expect(newest_message.reload.locked?).to be false
          end

          it "locks the next message at the same priority when another worker locks the oldest first" do
            oldest_message = create(:queued_message, server: high_priority_server)
            next_message = create(:queued_message, server: high_priority_server)
            create(:queued_message, server: low_priority_server)
            allow(job).to receive(:lock_message).and_wrap_original do |original, id|
              oldest_message.update_columns(locked_by: "another-worker", locked_at: Time.current) if id == oldest_message.id
              original.call(id)
            end
            job.call
            expect(MessageDequeuer).to have_received(:process).once
            expect(MessageDequeuer).to have_received(:process).with(next_message, logger: kind_of(Klogger::Logger))
          end

          it "gives up, without dropping to a lower priority, when every lock attempt loses to another worker" do
            create(:queued_message, server: high_priority_server)
            low_priority_message = create(:queued_message, server: low_priority_server)
            allow(job).to receive(:lock_message).and_return(false)
            job.call
            expect(job).to have_received(:lock_message).exactly(described_class::LOCK_ROUNDS).times
            expect(MessageDequeuer).to_not have_received(:process)
            expect(low_priority_message.reload.locked?).to be false
          end

          it "locks a lower priority message when no higher priority message is ready" do
            create(:queued_message, :locked, server: high_priority_server)
            create(:queued_message, :retry_in_future, server: high_priority_server)
            create(:queued_message, server: high_priority_server, retry_after: 10.seconds.ago)
            low_priority_message = create(:queued_message, server: low_priority_server)
            job.call
            expect(MessageDequeuer).to have_received(:process).once
            expect(MessageDequeuer).to have_received(:process).with(low_priority_message, logger: kind_of(Klogger::Logger))
          end

          context "when the worker has a minimum priority" do
            before do
              allow(Postal::Config.worker).to receive(:minimum_priority).and_return(50)
            end

            it "does not lock messages below the minimum priority" do
              low_priority_message = create(:queued_message, server: low_priority_server)
              job.call
              expect(MessageDequeuer).to_not have_received(:process)
              expect(low_priority_message.reload.locked?).to be false
            end

            it "locks messages at the minimum priority" do
              create(:queued_message, server: low_priority_server)
              queued_message = create(:queued_message, server: create(:server, priority: 50))
              job.call
              expect(MessageDequeuer).to have_received(:process).once
              expect(MessageDequeuer).to have_received(:process).with(queued_message, logger: kind_of(Klogger::Logger))
            end
          end
        end
      end
    end

  end
end
