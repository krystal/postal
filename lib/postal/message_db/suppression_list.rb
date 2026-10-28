# frozen_string_literal: true

module Postal
  module MessageDB
    class SuppressionList

      def initialize(database)
        @database = database
      end

      def add(type, address, options = {})
        keep_until = (options[:days] || Postal::Config.postal.default_suppression_list_automatic_removal_days).days.from_now.to_f
        if existing = @database.select("suppressions", where: { type: type, address: address }, limit: 1).first
          reason = options[:reason] || existing["reason"]
          @database.update("suppressions", { reason: reason, keep_until: keep_until }, where: { id: existing["id"] })
        else
          @database.insert("suppressions", { type: type, address: address, reason: options[:reason], timestamp: Time.now.to_f, keep_until: keep_until })
        end
        true
      end

      def get(type, address)
        @database.select("suppressions", where: { type: type, address: address, keep_until: { greater_than_or_equal_to: Time.now.to_f } }, limit: 1).first
      end

      def all_with_pagination(page)
        @database.select_with_pagination(:suppressions, page, order: :timestamp, direction: "desc")
      end

      def remove(type, address)
        @database.delete("suppressions", where: { type: type, address: address }).positive?
      end

      def prune
        @database.delete("suppressions", where: { keep_until: { less_than: Time.now.to_f } }) || 0
      end

      # Copy unexpired suppressions from another list. An existing suppression for the same
      # type and address is only extended if the copied one expires later, so it is safe to re-run.
      def copy_from(other)
        counts = { added: 0, extended: 0, unchanged: 0 }
        other.unexpired.each do |suppression|
          existing = @database.select("suppressions", where: { type: suppression["type"], address: suppression["address"] }, limit: 1).first
          if existing.nil?
            @database.insert("suppressions", suppression.slice("type", "address", "reason", "timestamp", "keep_until"))
            counts[:added] += 1
          elsif existing["keep_until"].nil? || existing["keep_until"] < suppression["keep_until"]
            @database.update("suppressions", { reason: suppression["reason"] || existing["reason"], keep_until: suppression["keep_until"] }, where: { id: existing["id"] })
            counts[:extended] += 1
          else
            counts[:unchanged] += 1
          end
        end
        counts
      end

      def unexpired
        @database.select("suppressions", where: { keep_until: { greater_than_or_equal_to: Time.now.to_f } }, order: :id)
      end

    end
  end
end
