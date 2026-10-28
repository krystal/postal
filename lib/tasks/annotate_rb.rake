# frozen_string_literal: true

# This rake task was added by annotate_rb gem.

# Can set `ANNOTATERB_SKIP_ON_DB_TASKS` to true, t, yes, y or 1 (case-insensitive) to skip this
if Rails.env.development? && !%w[true t yes y 1].include?(ENV["ANNOTATERB_SKIP_ON_DB_TASKS"].to_s.downcase)
  require "annotate_rb"

  # Can modify the config path here if needed - by default, it's .annotaterb.yml in the root of the project
  # AnnotateRb::ConfigFinder.config_path = ""
  AnnotateRb::Core.load_rake_tasks
end
