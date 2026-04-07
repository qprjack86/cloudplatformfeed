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
checksums_json_text = read_text(ROOT.join("data", "checksums.json"))
readme_text = read_text(ROOT.join("README.md"))
cname_text = read_text(ROOT.join("CNAME"))
headers_text = read_text(ROOT.join("_headers"))
fetch_script = read_text(ROOT.join("scripts", "fetch_feeds.py"))
site_config_text = read_text(ROOT.join("config", "site.json"))

site_config = begin
  JSON.parse(site_config_text)
rescue JSON::ParserError => e
  fail_check("config/site.json is invalid JSON: #{e.message}")
end

canonical_host = site_config["canonicalHost"]
canonical_url = site_config["canonicalUrl"]
csp_match = headers_text.match(/Content-Security-Policy:\s*(.+)$/)
csp_policy = csp_match && csp_match[1]
assert(canonical_host.is_a?(String) && !canonical_host.empty?, "config/site.json canonicalHost must be a non-empty string")
assert(canonical_url == "https://#{canonical_host}", "config/site.json canonicalUrl must match canonicalHost")
assert(csp_policy.is_a?(String) && !csp_policy.empty?, "_headers must include a Content-Security-Policy value")
assert(index_html.include?("content=\"#{canonical_url}\""), "index.html og:url must match config/site.json canonicalUrl")
assert(index_html.include?("content=\"#{canonical_url}/og-image.png\""), "index.html og:image must match config/site.json canonicalUrl")
assert(readme_text.include?("[#{canonical_host}](#{canonical_url})"), "README live-site link must match config/site.json canonical URL")
assert(readme_text.include?("`#{canonical_url}`"), "README setup URL must match config/site.json canonical URL")
assert(feed_xml.include?("<link>#{canonical_url}</link>"), "data/feed.xml channel link must match config/site.json canonicalUrl")
assert(fetch_script.include?("load_site_config"), "fetch_feeds.py must load canonical URL from config/site.json")
assert(!fetch_script.include?(canonical_host), "fetch_feeds.py should not hardcode canonical host")

[index_html, app_js].each do |content|
  assert(!contains_conflict_markers?(content), "merge conflict markers detected")
end

assert(
  index_html.match?(%r{<script\s+src="js/app\.js(?:\?[^"]+)?"\s+defer></script>}),
  "index.html does not load js/app.js with defer"
)
assert(!index_html.include?("js/clarity.js"), "index.html should not reference removed js/clarity.js")
assert(index_html.include?("Content-Security-Policy"), "index.html is missing a Content-Security-Policy")
assert(index_html.include?("style-src 'self'"), "index.html CSP should restrict styles to self")
assert(!index_html.include?("style-src 'self' 'unsafe-inline'"), "index.html CSP should not allow unsafe-inline styles")
assert(!index_html.include?("script-src 'self' 'unsafe-inline'"), "index.html CSP should not allow unsafe-inline scripts")
assert(!index_html.include?("www.clarity.ms/tag/\"+i"), "index.html should not inline the Clarity bootstrap")
assert(index_html.include?("id=\"articles-grid\""), "index.html is missing articles grid container")
assert(index_html.include?("id=\"filter-pills\""), "index.html is missing filter pills container")
assert(headers_text.include?("Content-Security-Policy:"), "_headers must include Content-Security-Policy")
assert(headers_text.include?("default-src 'self'"), "_headers CSP must include default-src 'self'")
assert(headers_text.include?("Strict-Transport-Security:"), "_headers must include Strict-Transport-Security")
assert(headers_text.match?(/Strict-Transport-Security:\s*max-age=31536000/i), "_headers HSTS max-age must be at least one year")
assert(headers_text.include?("includeSubDomains"), "_headers HSTS policy must include includeSubDomains")

feeds = JSON.parse(feeds_json_text)
checksums = JSON.parse(checksums_json_text)
articles = feeds["articles"]

assert(articles.is_a?(Array), "feeds.json articles must be an array")
assert(!articles.empty?, "feeds.json articles must not be empty")
assert(feeds["lastUpdated"].is_a?(String) && !feeds["lastUpdated"].empty?, "feeds.json lastUpdated is missing")
assert(feed_xml.include?("<rss") || feed_xml.include?("<feed"), "feed.xml does not look like RSS or Atom")


assert(checksums["generatedAt"].is_a?(String) && !checksums["generatedAt"].empty?, "checksums.json generatedAt is missing")
assert(checksums["artifacts"].is_a?(Array) && !checksums["artifacts"].empty?, "checksums.json artifacts must be a non-empty array")
expected_artifacts = ["data/feeds.json", "data/feed.xml"]
artifact_paths = checksums["artifacts"].map { |artifact| artifact["path"] }
expected_artifacts.each do |expected_path|
  assert(artifact_paths.include?(expected_path), "checksums.json must include #{expected_path}")
end
checksums["artifacts"].each_with_index do |artifact, index|
  assert(artifact.is_a?(Hash), "checksums artifact #{index} must be an object")
  assert(artifact["path"].is_a?(String) && !artifact["path"].empty?, "checksums artifact #{index} path is missing")
  assert(artifact["algorithm"] == "sha256", "checksums artifact #{index} algorithm must be sha256")
  assert(artifact["value"].is_a?(String) && artifact["value"].match?(/\A\h{64}\z/), "checksums artifact #{index} value must be a 64-character hex SHA-256")
  assert(artifact["generatedAt"].is_a?(String) && !artifact["generatedAt"].empty?, "checksums artifact #{index} generatedAt is missing")
end

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
