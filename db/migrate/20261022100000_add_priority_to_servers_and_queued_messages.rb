# frozen_string_literal: true

class AddPriorityToServersAndQueuedMessages < ActiveRecord::Migration[7.0]

  def change
    add_column :servers, :priority, :integer, null: false, default: 50
    add_column :queued_messages, :priority, :integer, null: false, default: 50
    add_index :queued_messages, :priority
  end

end
