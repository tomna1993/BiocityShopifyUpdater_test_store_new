require 'net/http'
require 'json'
require "time"
require 'uri'
require 'zlib'
require 'openssl'


def honama_helper_bulk_poll(shop, token, op_type: 'QUERY', sleep: 5, log_prefix: "shop", flag_exit_when_running: false, flag_verbose: true)
  last_status = nil

  # Ask Shopify for all the fields you actually log:
  q = <<~GRAPHQL
    {
      currentBulkOperation(type: #{op_type}) {
        id
        status
        url
        errorCode
        objectCount
        fileSize
      }
    }
  GRAPHQL

  status_messages = {
    "CREATED"   => "Bulk operation has been created and queued, but not started yet.",
    "RUNNING"   => "Bulk operation is currently running.",
    "CANCELING" => "Cancellation has been requested; Shopify is in the process of stopping the job.",
    "CANCELED"  => "Bulk operation has been canceled. No further work will be done and there is no result file.",
    "COMPLETED" => "Bulk operation completed successfully; the result file is ready at `url`.",
    "FAILED"    => "Bulk operation has failed. Check `errorCode` for the reason.",
    "EXPIRED"   => "Bulk operation completed, but the result file URL has expired. You need to re-run the operation."
  }

  loop do
    data = honama_helper_shopify_api(shop, token, { "query" => q })
    op = data.dig("data", "currentBulkOperation")

    if op
      status = op["status"]

      if status != last_status
        explanation = status_messages[status] || "Unknown status returned by Shopify."

        if flag_verbose
          puts "#{log_prefix}  [bulk] status=#{status} objects=#{op['objectCount']} " \
               "fileSize=#{op['fileSize']} url=#{!!op['url']}"
          puts "#{log_prefix}  [bulk] #{explanation}"
        end

        last_status = status

        case status
        when "COMPLETED"
          # Success – caller can read op["url"]
          return op
        when "FAILED"
          raise("Bulk failed: #{op&.dig('errorCode')}")
        when "CANCELED"
          raise("Bulk cancelled: #{op&.dig('errorCode')}")
        when "EXPIRED"
          # Completed but URL is no longer valid – return so caller can decide what to do
          return op
        when "RUNNING"
          return op if flag_exit_when_running
        # CREATED, RUNNING, CANCELING → just keep polling
        end
      end
    else
      puts "#{log_prefix}  [bulk] No currentBulkOperation (yet?)" if flag_verbose
    end

    puts "#{log_prefix}  sleep: #{sleep}s" if flag_verbose
    sleep(sleep)
    sleep *= 2
  end
end

def honama_helper_bulk_each_line(url)
  uri = URI(url)

  # follow up to 5 redirects
  10.times do
    Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https") do |http|
      req = Net::HTTP::Get.new(uri.request_uri)
      http.request(req) do |res|
        if res.is_a?(Net::HTTPRedirection)
          uri = URI(res["location"])
          next
        end

        buffer = +""
        first = true
        gunzip = false
        inflater = nil

        res.read_body do |chunk|
          if first
            first = false
            # detect gzip by magic bytes (not only by header)
            gunzip = chunk.bytes[0,2] == [0x1F, 0x8B]
            inflater = Zlib::Inflate.new(Zlib::MAX_WBITS + 32) if gunzip
          end

          data = gunzip ? inflater.inflate(chunk) : chunk
          buffer << data

          while (idx = buffer.index("\n"))
            line = buffer.slice!(0..idx).strip
            next if line.empty?
            yield JSON.parse(line)
          end
        end

        if gunzip
          rest = inflater.finish rescue +""
          inflater.close rescue nil
          buffer << rest
        end

        unless buffer.strip.empty?
          yield JSON.parse(buffer.strip)
        end
        return
      end
    end
  end
  raise "Too many redirects for bulk URL"
end

#-----------------------------------------------------------
# GET PRODUCTS
GET_PRODUCT_DATA_QUERY = <<~GRAPHQL
  mutation {
    bulkOperationRunQuery(query: """
      {
        products {
          edges {
            node {
              id
              variants {
                edges {
                  node {
                    id
                    barcode
                    inventoryItem {
                      id
                      inventoryLevels(first: 1) {
                        edges {
                          node {
                            location {
                              id
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    """) {
      bulkOperation { id status }
      userErrors { field message }
    }
  }
GRAPHQL



#-----------------------------------------------------------
# UPDATE PRICES
def honama_helper_bulk_multipart_body(fields, file_field_name:, filename:, content_type:, file_bytes:)
  boundary = "----RubyMultipart#{rand(1_000_000_000)}"
  lines = []

  # regular fields
  fields.each do |name, value|
    lines << "--#{boundary}"
    lines << %(Content-Disposition: form-data; name="#{name}")
    lines << ''
    lines << value.to_s
  end

  # file field must be LAST
  lines << "--#{boundary}"
  lines << %(Content-Disposition: form-data; name="#{file_field_name}"; filename="#{filename}")
  lines << %(Content-Type: #{content_type})
  lines << ''
  body_head = lines.join("\r\n")
  body_tail = "\r\n--#{boundary}--\r\n"

  # assemble final body as binary string
  body = String.new
  body << body_head
  body << "\r\n"
  body << file_bytes
  body << body_tail

  [body, boundary]
end

def honama_helper_bulk_multipart_post(url_str, fields, file_field_name:, filename:, content_type:, file_bytes:)
  uri = URI(url_str)
  body, boundary = honama_helper_bulk_multipart_body(fields, file_field_name: file_field_name, filename: filename, content_type: content_type, file_bytes: file_bytes)

  req = Net::HTTP::Post.new(uri)
  req['Content-Type'] = "multipart/form-data; boundary=#{boundary}"
  req['Content-Length'] = body.bytesize.to_s
  req.body = body

  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = uri.scheme == 'https'
  http.read_timeout = 300
  res = http.request(req)
  unless res.is_a?(Net::HTTPSuccess) || res.is_a?(Net::HTTPCreated)
    raise "Upload failed (#{res.code}): #{res.body}"
  end
  res
end

# ---------------------------
# Bulk upload flow
# ---------------------------

STAGED_UPLOAD_MUTATION = <<~GRAPHQL
  mutation {
    stagedUploadsCreate(input: [{
      resource: BULK_MUTATION_VARIABLES,
      filename: "bulk_op_vars",
      mimeType: "text/jsonl",
      httpMethod: POST
    }]) {
      userErrors { field message }
      stagedTargets {
        url
        resourceUrl
        parameters { name value }
      }
    }
  }
GRAPHQL

def honama_helper_bulk_staged_uploads(shop, token )
  data = honama_helper_shopify_api( shop, token, { "query" => STAGED_UPLOAD_MUTATION })
  if data['errors']
    raise "GraphQL errors: #{ JSON.pretty_generate( data['errors'] ) }"
  end
  errs = data.dig('data', 'stagedUploadsCreate', 'userErrors') || []
  unless errs.empty?
    raise "stagedUploadsCreate errors: #{errs.map { |e| e['message'] }.join('; ')}"
  end
  targets = data.dig('data', 'stagedUploadsCreate', 'stagedTargets') || []
  raise 'No stagedTargets returned' if targets.empty?
  targets.first # => { "url" => "...", "parameters" => [ {"name"=>"key","value"=>"tmp/.../bulk_op_vars"}, ... ] }
end

def honama_helper_bulk_upload_staged_target( staged_target, jsonl_bytes )
  url = staged_target.fetch('url')
  params = staged_target.fetch('parameters') # array of {"name","value"}
  fields = {}
  params.each { |p| fields[p['name']] = p['value'] }

  # Per docs: the file field must be LAST, and param name is "file"
  honama_helper_bulk_multipart_post(
    url,
    fields,
    file_field_name: 'file',
    filename: 'bulk_op_vars',
    content_type: 'text/jsonl',
    file_bytes: jsonl_bytes
  )
end

# The actual mutation string we'll pass to bulkOperationRunMutation.
# IMPORTANT: its variables MUST match the JSONL per-line object keys.
BULK_MUTATION_STRING = %(
  mutation bulkPrice($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
      product { id }
      productVariants { id }
      userErrors { field message }
    }
  }
)

BULK_MUTATION_SET_QUANTITIES = %(
  mutation inventorySet($input: InventorySetQuantitiesInput!) {
    inventorySetQuantities(input: $input) {
      inventoryAdjustmentGroup { id }
      userErrors { field message code }
    }
  }
)

RUN_BULK_MUTATION = <<~GRAPHQL
  mutation($mutation: String!, $stagedUploadPath: String!) {
    bulkOperationRunMutation(
      mutation: $mutation,
      stagedUploadPath: $stagedUploadPath
    ) {
      bulkOperation { id status url }
      userErrors { field message }
    }
  }
GRAPHQL

def honama_helper_bulk_run_mutation(shop, token, mutation_string, staged_upload_key)
  vars = { mutation: mutation_string, stagedUploadPath: staged_upload_key }
  data = honama_helper_shopify_api(shop, token, { "query" => RUN_BULK_MUTATION, "variables" => vars })
  if data['errors']
    raise "GraphQL errors: #{JSON.pretty_generate(data['errors'])}"
  end
  errs = data.dig('data', 'bulkOperationRunMutation', 'userErrors') || []
  unless errs.empty?
    raise "bulkOperationRunMutation errors: #{errs.map { |e| e['message'] }.join('; ')}"
  end
  data.dig('data', 'bulkOperationRunMutation', 'bulkOperation')
end

CURRENT_BULK = <<~GRAPHQL
  query {
    currentBulkOperation(type: MUTATION) {
      id
      status
      errorCode
      createdAt
      completedAt
      objectCount
      fileSize
      url
      partialDataUrl
    }
  }
GRAPHQL

# ---------------------------
# JSONL building
# ---------------------------

# product_data: array of [product_gid, variant_gid, price]
# For productVariantsBulkUpdate each JSONL line must hold both variables:
#   { "productId": "gid://shopify/Product/...", "variants": [ { "id": "gid://shopify/ProductVariant/...", "price": "12.34" }, ... ] }
#
def honama_helper_bulk_build_prices( product_data )
  grouped = Hash.new { |h, k| h[k] = [] }
  product_data.each do |prod_id, var_id, price|
    # price as Money scalar: send as string for safety
    grouped[prod_id] << { id: var_id, price: sprintf('%.2f', price.to_f) }
  end

  # Each line: one productId and its variants array
  # Keep lines small; Shopify processes each line independently.
  out = String.new
  grouped.each do |prod_id, variants|
    row = { productId: prod_id, variants: variants }
    out << JSON.generate(row) << "\n"
  end
  out
end

# variant_updates: array of [product_gid, variant_gid, price, inventory_policy, tracked]
def honama_helper_bulk_build_variant_updates( variant_updates, default_inventory_policy: "CONTINUE" )
  grouped = Hash.new { |h, k| h[k] = [] }
  variant_updates.each do |prod_id, var_id, price, inventory_policy, tracked|
    row = { id: var_id }
    row[:price] = sprintf('%.2f', price.to_f) if !price.nil?

    policy = inventory_policy || default_inventory_policy
    row[:inventoryPolicy] = policy if policy

    if tracked == true || tracked == false
      row[:inventoryItem] = { tracked: tracked }
    end

    pp row

    grouped[prod_id] << row
  end

  out = String.new
  grouped.each do |prod_id, variants|
    row = { productId: prod_id, variants: variants }
    out << JSON.generate(row) << "\n"
  end
  out
end

# quantity_updates: array of [inventory_item_gid, location_gid, quantity]
def honama_helper_bulk_build_inventory_set_quantities( quantity_updates, batch_size: 250, ignore_compare: true, reason: "correction", name: "available", reference_document_uri: nil )
  out = String.new
  quantity_updates.each_slice(batch_size) do |slice|
    quantities = slice.map do |inventory_item_id, location_id, quantity|
      {
        inventoryItemId: inventory_item_id,
        locationId: location_id,
        quantity: quantity.to_i
      }
    end

    input = {
      name: name,
      reason: reason,
      quantities: quantities
    }
    input[:ignoreCompareQuantity] = true if ignore_compare
    input[:referenceDocumentUri] = reference_document_uri if reference_document_uri

    out << JSON.generate({ input: input }) << "\n"
  end
  out
end


# ----------------------
# --- PUBLIC ENTRIES ---
# ----------------------

def honama_helper_bulk_get_products( store, token, log_prefix: "shop", flag_verbose: true )
  res = honama_helper_shopify_api(store, token, { "query" => GET_PRODUCT_DATA_QUERY })
  op = honama_helper_bulk_poll( store, token, log_prefix: log_prefix, flag_verbose: flag_verbose )   # pass shop/token/api_url like you do now

  pp op
  variants = []
  variants_by_id = {}
  variant_to_inventory_item = {}
  inventory_item_to_variant = {}
  inventory_item_to_inventory_level = {}
  inventory_level_to_location = {}
  variant_to_location = {}

  honama_helper_bulk_each_line(op["url"]) do |row|
    id = row["id"].to_s

    if row.key?("location")
      location_id = row.dig("location", "id")
      parent_id = row["__parentId"]
      if parent_id&.include?("ProductVariant")
        variant_to_location[parent_id] = location_id
      else
        inventory_level_to_location[parent_id] = location_id
      end
      next
    end

    case id
    when /ProductVariant/
      inventory_item_id = row.dig("inventoryItem", "id")
      variants_by_id[id] = {
        product_id: row["__parentId"],
        id: id,
        barcode: row["barcode"],
        inventory_item_id: inventory_item_id
      }
      variant_to_inventory_item[id] = inventory_item_id if inventory_item_id
      inventory_item_to_variant[inventory_item_id] = id if inventory_item_id
    when /InventoryItem/
      inventory_item_to_variant[id] = row["__parentId"]
      variant_to_inventory_item[row["__parentId"]] = id
    when /InventoryLevel/
      inventory_item_to_inventory_level[row["__parentId"]] = id
    when /location/
      inventory_level_to_location[row["__parentId"]] = id
    end
  end

  variants_by_id.each_value do |v|
    inventory_item_id = v[:inventory_item_id] || variant_to_inventory_item[v[:id]]
    inventory_level_id = inventory_item_to_inventory_level[inventory_item_id]
    location_id = variant_to_location[v[:id]] || inventory_level_to_location[inventory_level_id]

    variants << [
      v[:product_id],
      v[:id],
      v[:barcode],
      location_id,
      inventory_item_id
    ]
  end
  return variants
end

def honama_helper_bulk_update_prices( shop, token, product_data, log_prefix: "shop", flag_verbose: true )
  
  # wait until the previous mutation finishes
  done = honama_helper_bulk_poll( shop, token, op_type: 'MUTATION', log_prefix: log_prefix, flag_exit_when_running: false, flag_verbose: flag_verbose )
  puts "#{ log_prefix }  [done] status=#{done['status']} errorCode=#{done['errorCode']} objectCount=#{done['objectCount']}"  if flag_verbose
  return { "objectCount" => 0 } if done["status"] == "RUNNING"
  
  # 1) Build JSONL
  jsonl = honama_helper_bulk_build_prices( product_data )
  bytesize = jsonl.bytesize
  if bytesize > 20 * 1024 * 1024
    raise "JSONL is #{bytesize} bytes (>20MB limit). Split your payload."
  end
  puts "#{ log_prefix }  [jsonl] lines=#{jsonl.count("\n")} size=#{bytesize} bytes"  if flag_verbose

  # 2) stagedUploadsCreate
  staged_target = honama_helper_bulk_staged_uploads( shop, token )
  params = staged_target.fetch('parameters')
  key_param = params.find { |p| p['name'] == 'key' } or raise 'No key in staged parameters'
  staged_key = key_param['value']
  puts "#{ log_prefix }  [staged] key=#{staged_key}"  if flag_verbose

  # 3) Upload JSONL (multipart; file must be last)
  honama_helper_bulk_upload_staged_target( staged_target, jsonl )

  # 4) Start the bulk mutation
  op = honama_helper_bulk_run_mutation(shop, token, BULK_MUTATION_STRING, staged_key)
  puts "#{ log_prefix }  [run] bulkOperation id=#{op['id']} status=#{op['status']}"  if flag_verbose

  # 5) Poll until completion
  done = honama_helper_bulk_poll( shop, token, log_prefix: log_prefix, flag_exit_when_running: false, flag_verbose: flag_verbose )
  puts "#{ log_prefix }  [done] status=#{done['status']} errorCode=#{done['errorCode']} objectCount=#{done['objectCount']}"  if flag_verbose

  # 6) If COMPLETED, a result JSONL exists (mostly userErrors per line). Download if you want.
  if done['url']
    puts "#{ log_prefix }  [result] Download URL (expires): #{done['url']}"  if flag_verbose
  elsif done['partialDataUrl']
    puts "#{ log_prefix }  [result] Partial data URL (expires): #{done['partialDataUrl']}"  if flag_verbose
  end

  #return done
  return done['objectCount']
end

def honama_helper_bulk_update_variants( shop, token, variant_updates, log_prefix: "shop", flag_verbose: true )
  return 0 if variant_updates.size <= 0

  # wait until the previous mutation finishes
  done = honama_helper_bulk_poll( shop, token, op_type: 'MUTATION', log_prefix: log_prefix, flag_exit_when_running: false, flag_verbose: flag_verbose )
  puts "#{ log_prefix }  [done] status=#{done['status']} errorCode=#{done['errorCode']} objectCount=#{done['objectCount']}"  if flag_verbose
  return { "objectCount" => 0 } if done["status"] == "RUNNING"


  jsonl = honama_helper_bulk_build_variant_updates( variant_updates )
  bytesize = jsonl.bytesize
  if bytesize > 20 * 1024 * 1024
    raise "JSONL is #{bytesize} bytes (>20MB limit). Split your payload."
  end
  puts "#{ log_prefix }  [jsonl] lines=#{jsonl.count("\n")} size=#{bytesize} bytes"  if flag_verbose

  staged_target = honama_helper_bulk_staged_uploads( shop, token )
  params = staged_target.fetch('parameters')
  key_param = params.find { |p| p['name'] == 'key' } or raise 'No key in staged parameters'
  staged_key = key_param['value']
  puts "#{ log_prefix }  [staged] key=#{staged_key}"  if flag_verbose

  honama_helper_bulk_upload_staged_target( staged_target, jsonl )

  op = honama_helper_bulk_run_mutation(shop, token, BULK_MUTATION_STRING, staged_key)
  puts "#{ log_prefix }  [run] bulkOperation id=#{op['id']} status=#{op['status']}"  if flag_verbose

  done = honama_helper_bulk_poll( shop, token, log_prefix: log_prefix, flag_verbose: flag_verbose )
  puts "#{ log_prefix }  [done] status=#{done['status']} errorCode=#{done['errorCode']} objectCount=#{done['objectCount']}"  if flag_verbose

  if done['url']
    puts "#{ log_prefix }  [result] Download URL (expires): #{done['url']}"  if flag_verbose
  elsif done['partialDataUrl']
    puts "#{ log_prefix }  [result] Partial data URL (expires): #{done['partialDataUrl']}"  if flag_verbose
  end

  done['objectCount']
end

def honama_helper_bulk_update_quantities( shop, token, quantity_updates, log_prefix: "shop", flag_verbose: true )
  return 0 if quantity_updates.size <= 0

# wait until the previous mutation finishes
  done = honama_helper_bulk_poll( shop, token, op_type: 'MUTATION', log_prefix: log_prefix, flag_exit_when_running: false, flag_verbose: flag_verbose )
  puts "#{ log_prefix }  [done] status=#{done['status']} errorCode=#{done['errorCode']} objectCount=#{done['objectCount']}"  if flag_verbose
  return { "objectCount" => 0 } if done["status"] == "RUNNING"

  jsonl = honama_helper_bulk_build_inventory_set_quantities( quantity_updates )
  bytesize = jsonl.bytesize
  if bytesize > 20 * 1024 * 1024
    raise "JSONL is #{bytesize} bytes (>20MB limit). Split your payload."
  end
  puts "#{ log_prefix }  [jsonl] lines=#{jsonl.count("\n")} size=#{bytesize} bytes"  if flag_verbose

  staged_target = honama_helper_bulk_staged_uploads( shop, token )
  params = staged_target.fetch('parameters')
  key_param = params.find { |p| p['name'] == 'key' } or raise 'No key in staged parameters'
  staged_key = key_param['value']
  puts "#{ log_prefix }  [staged] key=#{staged_key}"  if flag_verbose

  honama_helper_bulk_upload_staged_target( staged_target, jsonl )

  op = honama_helper_bulk_run_mutation(shop, token, BULK_MUTATION_SET_QUANTITIES, staged_key)
  puts "#{ log_prefix }  [run] bulkOperation id=#{op['id']} status=#{op['status']}"  if flag_verbose

  done = honama_helper_bulk_poll( shop, token, log_prefix: log_prefix, flag_exit_when_running: false, flag_verbose: flag_verbose )
  puts "#{ log_prefix }  [done] status=#{done['status']} errorCode=#{done['errorCode']} objectCount=#{done['objectCount']}"  if flag_verbose

  if done['url']
    puts "#{ log_prefix }  [result] Download URL (expires): #{done['url']}"  if flag_verbose
  elsif done['partialDataUrl']
    puts "#{ log_prefix }  [result] Partial data URL (expires): #{done['partialDataUrl']}"  if flag_verbose
  end

  done['objectCount']
end


# -------------------
# --- JOB ENTRIES ---
# -------------------

def honama_helper_shopify_api_active_items2( shop, token, log_prefix: "shop", flag_verbose: true )
	res = []
	log_prefix = shop  if not log_prefix
	#shop_name_real, shop2 = honama_helper_store_shop_name2( shop )

  # temp:
  # product_id, variant_id, barcode, location_id
	temp = honama_helper_bulk_get_products( shop, token, log_prefix: log_prefix, flag_verbose: flag_verbose )
	temp.each{|x|
		product_id = x[0].to_s[/[^\/]+$/i].to_s
		variant_id = x[1].to_s[/[^\/]+$/i].to_s
		barcode = x[2]
		location_id = x[3].to_s[/[^\/]+$/i].to_s
    inventory_id = x[4].to_s[/[^\/]+$/i].to_s

		res << [ product_id, variant_id, barcode, location_id, inventory_id ]
	}
	return res
end

def honama_helper_shopify_api_update_prices2( price_new_all, shop, token, log_prefix: "shop", flag_verbose: true )
	return 0  if price_new_all.size <= 0

	# prod_id, var_id, price
	product_data = []
	price_new_all.each{|gid,id,price_new|
		product_data << [
			"gid://shopify/Product/#{ gid }",
			"gid://shopify/ProductVariant/#{ id }",
			price_new,
		]
	}
	# product_data.shuffle! # done by shopify bulk update logic

	honama_helper_log( "#{ log_prefix }  items to update #{ product_data.size }" )

	count_update_total = honama_helper_bulk_update_prices( shop, token, product_data, log_prefix: log_prefix, flag_verbose: flag_verbose )

	return count_update_total
end
