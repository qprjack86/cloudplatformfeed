#!/usr/bin/env ruby

require "json"
require "pathname"

ROOT = Pathname.new(__dir__).join("..").expand_path

def fail_check(message)
  warn "SMOKE TEST FAILED: #{message}"
  exit 1
end

def read_text(path)
  File.read(path)
rescue Errno::ENOENT
  fail_check("missing file #{path}")
end

def assert(condition, message)
  fail_check(message) unless condition
end

def contains_conflict_markers?(content)
  content.each_line.any? do |line|
    line.match?(/^(<<<<<<<|=======|>>>>>>>)/)
  end
end

index_html = read_text(ROOT.join("index.html"))
app_js = read_text(ROOT.join("js", "app.js"))
feeds_json_text = read_text(ROOT.join("data", "feeds.json"))
feed_xml = read_text(ROOT.join("data", "feed.xml"))

[index_html, app_js].each do |content|
  assert(!contains_conflict_markers?(content), "merge conflict markers detected")
end

assert(index_html.include?("<script src=\"js/app.js\" defer></script>"), "index.html does not load js/app.js with defer")
assert(index_html.include?("<script src=\"js/clarity.js\" defer></script>"), "index.html does not load js/clarity.js")
assert(index_html.include?("Content-Security-Policy"), "index.html is missing a Content-Security-Policy")
assert(!index_html.include?("www.clarity.ms/tag/\"+i"), "index.html should not inline the Clarity bootstrap")
assert(index_html.include?("id=\"articles-grid\""), "index.html is missing articles grid container")
assert(index_html.include?("id=\"filter-pills\""), "index.html is missing filter pills container")

feeds = JSON.parse(feeds_json_text)
articles = feeds["articles"]

assert(articles.is_a?(Array), "feeds.json articles must be an array")
assert(!articles.empty?, "feeds.json articles must not be empty")
assert(feeds["lastUpdated"].is_a?(String) && !feeds["lastUpdated"].empty?, "feeds.json lastUpdated is missing")
assert(feed_xml.include?("<rss") || feed_xml.include?("<feed"), "feed.xml does not look like RSS or Atom")

if feeds.key?("summary")
  assert(feeds["summary"].is_a?(String) && !feeds["summary"].strip.empty?, "feeds.json summary must be a non-empty string")
end

if feeds.key?("summaryWindowDays")
  assert(feeds["summaryWindowDays"].is_a?(Integer) && feeds["summaryWindowDays"] > 0, "feeds.json summaryWindowDays must be a positive integer")
end

if feeds.key?("summaryPublishingDays")
  assert(feeds["summaryPublishingDays"].is_a?(Array), "feeds.json summaryPublishingDays must be an array")
  feeds["summaryPublishingDays"].each_with_index do |day, index|
    assert(day.is_a?(String) && day.match?(/^\d{4}-\d{2}-\d{2}$/), "summaryPublishingDays entry #{index} must be YYYY-MM-DD")
  end
end

if feeds.key?("summaryStatus")
  assert(%w[available unavailable].include?(feeds["summaryStatus"]), "feeds.json summaryStatus must be available or unavailable")
end

if feeds.key?("summaryReason")
  allowed_reasons = %w[no_dated_articles no_articles_in_window missing_azure_openai_config azure_openai_failed]
  assert(allowed_reasons.include?(feeds["summaryReason"]), "feeds.json summaryReason must be an allowed public reason code")
end

assert(!feeds.key?("summaryError"), "feeds.json must not expose raw summaryError details")

if feeds.key?("summarySource")
  assert(feeds["summarySource"].is_a?(String) && !feeds["summarySource"].empty?, "feeds.json summarySource must be a non-empty string")
end

if feeds["summaryStatus"] == "available"
  assert(feeds.key?("summary"), "feeds.json summary must exist when summaryStatus is available")
end

required_keys = %w[title link published summary blog blogId author]

articles.each_with_index do |article, index|
  assert(article.is_a?(Hash), "article #{index} is not an object")

  required_keys.each do |key|
    value = article[key]
    assert(value.is_a?(String) && !value.strip.empty?, "article #{index} is missing #{key}")
  end
end

# Regression guard: Tech Community articles should always be present.
# If this drops to zero, the feed parser likely regressed for those sources.
techcommunity_articles = articles.select do |article|
  article["link"].include?("techcommunity.microsoft.com/t5/")
end
assert(!techcommunity_articles.empty?, "expected at least one Tech Community article in feeds.json")

techcommunity_articles.each_with_index do |article, index|
  assert(
    article["published"].match?(/^\d{4}-\d{2}-\d{2}T/),
    "Tech Community article #{index} has non-ISO published timestamp"
  )
end

puts "Smoke tests passed"