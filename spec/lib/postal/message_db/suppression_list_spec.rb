# frozen_string_literal: true

require "rails_helper"

describe Postal::MessageDB::SuppressionList do
  let(:server) { create(:server) }
  let(:database) { server.message_db }
  subject(:suppression_list) { database.suppression_list }

  def insert_suppression(database, type, address, reason: "hard fail", timestamp: 2.days.ago.to_i, keep_until: 10.days.from_now.to_i)
    database.insert("suppressions", { type: type, address: address, reason: reason, timestamp: timestamp, keep_until: keep_until })
  end

  def suppressions(database)
    database.select("suppressions", order: :id).map { |s| s.except("id") }
  end

  describe "#unexpired" do
    it "returns suppressions which have not expired" do
      insert_suppression(database, "recipient", "current@example.com", keep_until: 1.day.from_now.to_i)
      insert_suppression(database, "recipient", "expired@example.com", keep_until: 1.day.ago.to_i)
      expect(suppression_list.unexpired.map { |s| s["address"] }).to eq ["current@example.com"]
    end
  end

  describe "#copy_from" do
    # A separate source database, as each server has its own
    before(:context) do
      @source_database = Postal::MessageDB::Database.new(1, 2, database_name: "postal-test-message-db-source")
      @source_database.provisioner.provision
    end

    after(:context) do
      @source_database.provisioner.drop
    end

    after do
      @source_database.provisioner.clean
    end

    let(:source_database) { @source_database }
    let(:source) { source_database.suppression_list }

    it "copies unexpired suppressions with their reason, timestamp and expiry time" do
      timestamp = BigDecimal(3.days.ago.to_i) + BigDecimal("0.123456")
      keep_until = BigDecimal(20.days.from_now.to_i) + BigDecimal("0.654321")
      insert_suppression(source_database, "recipient", "bounced@example.com", reason: "too many hard fails", timestamp: timestamp, keep_until: keep_until)

      expect(suppression_list.copy_from(source)).to eq({ added: 1, extended: 0, unchanged: 0 })
      expect(suppressions(database)).to eq [
        { "type" => "recipient", "address" => "bounced@example.com", "reason" => "too many hard fails", "timestamp" => timestamp, "keep_until" => keep_until },
      ]
    end

    it "does not copy expired suppressions" do
      insert_suppression(source_database, "recipient", "expired@example.com", keep_until: 1.day.ago.to_i)
      expect(suppression_list.copy_from(source)).to eq({ added: 0, extended: 0, unchanged: 0 })
      expect(suppressions(database)).to eq []
    end

    it "does not create duplicates when run again" do
      insert_suppression(source_database, "recipient", "one@example.com")
      insert_suppression(source_database, "recipient", "two@example.com")
      suppression_list.copy_from(source)
      expect(suppression_list.copy_from(source)).to eq({ added: 0, extended: 0, unchanged: 2 })
      expect(suppressions(database).map { |s| s["address"] }).to eq ["one@example.com", "two@example.com"]
    end

    it "copies a suppression with the same address but a different type" do
      insert_suppression(database, "recipient", "user@example.com")
      insert_suppression(source_database, "sender", "user@example.com")
      expect(suppression_list.copy_from(source)).to eq({ added: 1, extended: 0, unchanged: 0 })
      expect(suppressions(database).map { |s| s["type"] }).to eq %w[recipient sender]
    end

    context "when the destination already has a suppression which expires sooner" do
      it "extends the existing suppression" do
        existing_timestamp = 5.days.ago.to_i
        later_keep_until = 30.days.from_now.to_i
        insert_suppression(database, "recipient", "user@example.com", reason: "soft fail", timestamp: existing_timestamp, keep_until: 5.days.from_now.to_i)
        insert_suppression(source_database, "recipient", "user@example.com", reason: "hard fail", keep_until: later_keep_until)

        expect(suppression_list.copy_from(source)).to eq({ added: 0, extended: 1, unchanged: 0 })
        expect(suppressions(database)).to eq [
          { "type" => "recipient", "address" => "user@example.com", "reason" => "hard fail", "timestamp" => existing_timestamp, "keep_until" => later_keep_until },
        ]
      end
    end

    context "when the destination already has a suppression which expires later" do
      it "leaves the existing suppression unchanged" do
        insert_suppression(database, "recipient", "user@example.com", reason: "soft fail", keep_until: 30.days.from_now.to_i)
        insert_suppression(source_database, "recipient", "user@example.com", reason: "hard fail", keep_until: 5.days.from_now.to_i)

        expect { suppression_list.copy_from(source) }.to_not(change { suppressions(database) })
        expect(suppression_list.copy_from(source)).to eq({ added: 0, extended: 0, unchanged: 1 })
      end
    end
  end
end
