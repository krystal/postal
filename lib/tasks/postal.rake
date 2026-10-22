# frozen_string_literal: true

namespace :postal do
  desc "Run all migrations on message databases"
  task migrate_message_databases: :environment do
    Server.all.each do |server|
      puts "Running migrations for #{server.organization.permalink}/#{server.permalink} (ID: #{server.id})"
      server.message_db.provisioner.migrate
    end
  end

  desc "Generate configuration documentation"
  task generate_config_docs: :environment do
    require "konfig/exporters/env_vars_as_markdown"

    FileUtils.mkdir_p("doc/config")
    output = Konfig::Exporters::EnvVarsAsMarkdown.new(Postal::ConfigSchema).export
    File.write("doc/config/environment-variables.md", output)

    output = Postal::YamlConfigExporter.new(Postal::ConfigSchema).export
    File.write("doc/config/yaml.yml", output)
  end

  desc "Generate Helm Environment Variables"
  task generate_helm_env_vars: :environment do
    puts Postal::HelmConfigExporter.new(Postal::ConfigSchema).export
  end

  desc "Copy unexpired suppressions from one server to another"
  task :copy_suppressions, [:from_server_id, :to_server_id] => :environment do |_, args|
    if args[:from_server_id].blank? || args[:to_server_id].blank?
      abort "Usage: rake postal:copy_suppressions[from_server_id,to_server_id]"
    end

    from = Server.find(args[:from_server_id])
    to = Server.find(args[:to_server_id])
    abort "The source and destination servers must be different" if from == to

    counts = to.message_db.suppression_list.copy_from(from.message_db.suppression_list)
    puts "Copied suppressions from #{from.full_permalink} (ID: #{from.id}) to #{to.full_permalink} (ID: #{to.id})"
    puts "#{counts[:added]} added, #{counts[:extended]} extended, #{counts[:unchanged]} already present"
  end

  desc "Update the database"
  task update: :environment do
    mysql = ActiveRecord::Base.connection
    if mysql.table_exists?("schema_migrations") &&
       mysql.select_all("select * from schema_migrations").any?
      puts "Database schema is already loaded. Running migrations with db:migrate"
      Rake::Task["db:migrate"].invoke
    else
      puts "No schema migrations exist. Loading schema with db:schema:load"
      Rake::Task["db:schema:load"].invoke
    end
  end
end

Rake::Task["db:migrate"].enhance do
  Rake::Task["postal:migrate_message_databases"].invoke
end
