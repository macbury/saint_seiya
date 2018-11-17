require 'rubygems'
require 'bundler/setup'
require 'dotenv/load'
require 'aws-sdk-athena'
require 'pry'
require 'terminal-table'

Aws.config.update({
  region: ENV.fetch('MY_AWS_REGION'),
  credentials: Aws::Credentials.new(
    ENV.fetch('MY_AWS_ACCESS_KEY_ID'),
    ENV.fetch('MY_AWS_SECRET_ACCESS_KEY')
  )
})

QUERY = "SELECT AVG(height) as avg_height, country FROM #{ENV.fetch('TABLE_NAME')}.countries GROUP BY country;"

@client = Aws::Athena::Client.new

query_resp = @client.start_query_execution(
  query_string: QUERY,
  query_execution_context: {
    database: ENV.fetch('DATABASE_NAME'),
  },
  result_configuration: {
    output_location: ENV.fetch('S3_BUCKET_OUTPUT')
  }
)

loop do
  status_resp = @client.get_query_execution(query_execution_id: query_resp.query_execution_id)
  puts "Query: #{query_resp.query_execution_id} status is: #{status_resp.query_execution.status.state}"
  break if ["SUCCEEDED", "FAILED", "CANCELLED"].include?(status_resp.query_execution.status.state)
  sleep 1
end

result_resp = @client.get_query_results(query_execution_id: query_resp.query_execution_id)
rows = result_resp.result_set.rows
                             .map { |row| [row.data[0].var_char_value, row.data[1].var_char_value] }
                             .reject { |row| row[0].nil? || row[0].empty? }

table = Terminal::Table.new(rows: rows[1..-1])
puts table
