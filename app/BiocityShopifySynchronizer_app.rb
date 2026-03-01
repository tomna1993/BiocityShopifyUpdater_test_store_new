require 'net/http'
require 'json'
require 'pp'
require 'set'
require 'fileutils'
require 'uri'
require 'csv'
require 'roo'
require 'roo-xls'
require 'spreadsheet'
require 'yaml'

require 'glimmer-dsl-libui'

LOG_FILE = 'log.txt'

require_relative 'honama_helper_shopify'
require_relative 'honama_helper2'

# --- HELPER FUNCTIONS ---
def honama_log( text, filename = LOG_FILE )
    f = File.open( filename, "a" )
    f.flock( File::LOCK_EX )
    f.write( (Time.now).to_s + "\t" + text.to_s + "\n")
    f.flush
    f.fsync
    f.close
end

class UpdaterLogic

    def initialize(log_lambda, progress_cb = nil)
        @log = log_lambda # Store the logger from the GUI

        @progress = progress_cb
    end

    def process_and_update_products(data_from_file_hash)
        start_time = Time.now
        @log.call("+++ Starting Update Process +++")

        @log.call("Get all product data from shopify...")

        @progress&.call(20)
        
        # puts "Data from file:"
        # pp data_from_file_hash
        
        # Get all products data
        # product_data_hash = get_product_data_from_shopify()
        product_data_hash = honama_helper_shopify_api_active_items2( SHOP, TOKEN, log_prefix: SHOP )
        
        # puts "Product data from Shopify:"
        # pp product_data_hash
        @progress&.call(40)

        update_product_hash = prepare_data_to_update_from_file(data_from_file_hash, product_data_hash)

        variant_updates = []
        quantity_updates = []

        update_product_hash.each do |barcode, details|
            product_id = details["productId"]
            variant_id = details["variantId"]
            inventory_item_id = details["inventoryItemId"]
            location_id = details["locationId"]

            if product_id && variant_id
                variant_updates << [
                    "gid://shopify/Product/#{product_id}",
                    "gid://shopify/ProductVariant/#{variant_id}",
                    details["price"],
                    "CONTINUE",
                    nil
                ]
            end

            if inventory_item_id && location_id
                quantity_updates << [
                    "gid://shopify/InventoryItem/#{inventory_item_id}",
                    "gid://shopify/Location/#{location_id}",
                    details["quantity"].to_i
                ]
            end
        end

        @progress&.call(50)
        
        
        @log.call("\nUpdating prices and inventory policy (CONTINUE)...")
        updated_variants = honama_helper_bulk_update_variants(SHOP, TOKEN, variant_updates, log_prefix: SHOP)
        @log.call("✅ Variants updated: #{updated_variants}")
        @progress&.call(80)
        
        @log.call("\nUpdating inventory quantities...")
        updated_quantities = honama_helper_bulk_update_quantities(SHOP, TOKEN, quantity_updates, log_prefix: SHOP)
        @log.call("✅ Inventory updates queued: #{updated_quantities}")
        @progress&.call(100)
        
        end_time = Time.now

        elapsed_time = end_time - start_time

        @log.call("\n✅ Process Completed Successfully!")
        @log.call("\nElapsed time: #{elapsed_time.round(0)} seconds")
    end

    # Load or save data from or to csv files depending on whether data is specified
    def honama_helper_csv(filename, data = nil)
      # Write data to a CSV file
      if data
        CSV.open(filename, "wb") do |csv|
          data.each { |row| csv << row }
        end
        return true
      # Load data from a CSV file
      else
        # Automatically detect the column separator
        options = { headers: true, col_sep: sniff_delimiter(filename) }

        # Read the file and convert it to a simple 2D array
        table = CSV.read(filename, **options)

        # Return the headers plus the data rows
        return [table.headers] + table.map { |row| row.fields }
      end
    end

    # A small helper function to auto-detect the delimiter
    def sniff_delimiter(filename)
      first_line = File.open(filename, &:readline)
      delimiters = [',', ';', "\t", '|']

      # Count occurrences of each delimiter in the first line
      counts = delimiters.map { |d| [first_line.count(d), d] }

      # Return the delimiter with the highest count
      return counts.max[1]
    end

    # Helper function to read .xls or .xlsx files
    def honama_helper_xls(filename, extension)
      # Open the spreadsheet file
	  Spreadsheet.client_encoding = 'CP1250'
      spreadsheet = Roo::Spreadsheet.open(filename, extension: extension.delete('.').to_sym)

      # Get the first worksheet
      sheet = spreadsheet.sheet(0)

	  array = sheet.map do |row|
		row.map{|cell| cell.is_a?(String)?cell.encode("UTF-8"):cell}
	  end

      # Return all rows as a simple 2D array.
      # This format ([header_row], [data_row_1], ...) matches your CSV helper's output.
      return array
    end

    # def get_product_data_from_shopify()
    #     # Get the keys from both hashes
    #     all_variants_data = {}
    #     has_next_page = true
    #     cursor = nil

    #     while has_next_page

    #         result = honama_helper_shopify_api(SHOP, TOKEN,
    #             {
    #                 "query" => GET_ALL_VARIANTS_QUERY,
    #                 "variables" => { 'cursor' => cursor }
    #             })

    #         # Check for top-level syntax errors first
    #         if result.key?("errors")
    #             top_level_errors = result["errors"]
    #             message = "get_product_data_from_shopify_by_barcode: API returned a top-level GraphQL error: #{top_level_errors}"
    #             honama_log(message)
    #             raise "❗️#{message}"

    #         # Iterate through each update operation (update1, update2, etc.)
    #         elsif result.key?("data")
    #             nodes = result.dig("data", "productVariants", "edges")

    #             if !nodes.empty?
	# 				nodes.each do |node_data|
    #                     variant_data = node_data['node']

	# 					if variant_data['barcode'].to_s.strip[/[^A-Za-z0-9]/i]
	# 						message = "get_product_data_from_shopify_by_barcode: Wrong barcode '#{variant_data['barcode']}' on shopify. There shouldn't be any special characters in barcode."
	# 						honama_log(message)
	# 						raise "❗️#{message}"
	# 					end
	# 					barcode = variant_data['barcode']

	# 					product_id = variant_data['product']['id']
    #                     status = variant_data['product']['status']
    #                     variant_id = variant_data['id']
    #                     inventoryItem_id = variant_data['inventoryItem']['id']
    #                     location_nodes = variant_data['inventoryItem']['inventoryLevels']['edges']

    #                     location_id = ""
	# 					location_nodes.each do |location_data|
	# 						location_id = location_data['node']['location']['id']
	# 					end

    #                     if barcode && !barcode.empty?
    #                         all_variants_data[barcode] = {
    #                             'status'            => status,
    #                             'productId'         => product_id,
    #                             'variantId'         => variant_id,
    #                             'inventoryItemId'   => inventoryItem_id,
    #                             'locationId'        => location_id
    #                         }
    #                     else
    #                         message = "get_product_data_from_shopify: Warning: Skipping variant with ID #{variant_data['id']} because it has no barcode."
	# 						honama_log(message)
	# 						@log.call("#{message}")
    #                     end
    #                 end
    #             end

    #             # Update loop control variables from pageInfo
    #             has_next_page = result['data']['productVariants']['pageInfo']['hasNextPage']
    #             cursor = result['data']['productVariants']['pageInfo']['endCursor'] if has_next_page
    #         else
    #             message = "get_product_data_from_shopify: Unidentified key in result: #{result}"
    #             honama_log(message)
    #             raise "#{message}"
    #         end
    #     end

    #     puts "Finished fetching. Total variants found: #{all_variants_data.length}"

    #     return all_variants_data
    # end

    # This function cleans a header string for reliable comparison.
    def normalize_header(header_string)
      return "" if header_string.nil?
      header_string
        .to_s           # Make sure it's a string
        .strip          # Remove leading/trailing whitespace
        .downcase       # Convert to lowercase
        .delete_prefix("\uFEFF") # Remove the BOM character
    end

    def prepare_data_from_file(input_file, columns_hash)
        # Preparing Data"
        unless File.exist?(input_file) && File.readable?(input_file)
            error_message = "prepare_data_from_file: Cannot read file: '#{File.basename(input_file)}'. It may be missing or locked by another program."
            honama_log(error_message)
            raise "❗ #{error_message}" # This error will be caught and displayed by run_update
        end

        # --- Load data based on file type ---
        csv_data = nil
        file_extension = File.extname(input_file).downcase

        if ['.xls', '.xlsx'].include?(file_extension)
            csv_data = honama_helper_xls(input_file, file_extension)
        elsif file_extension == '.csv'
            csv_data = honama_helper_csv(input_file)
        else
            error_message = "prepare_data_to_update_from_csv: Unsupported file type: #{input_file}. Please use .csv, .xls, or .xlsx."
            honama_log(error_message)
            raise "❗️#{error_message}"
        end

        # Get the first row, which contains the column headers
        header_row = csv_data[0]

        # Find the index for each column we need. .index returns the position of the element.
        barcode_index = header_row.index { |h| normalize_header(h) == normalize_header(columns_hash["vonalkod-oszlop"]) }
        quantity_index = header_row.index { |h| normalize_header(h) == normalize_header(columns_hash["mennyiseg-oszlop"]) }
        price_index = header_row.index { |h| normalize_header(h) == normalize_header(columns_hash["eladasi-ar-oszlop"]) }

        # Error handling: Stop the script if any required column is not found
        unless barcode_index && quantity_index && price_index
            error_message = "prepare_data_to_update_from_file: Missing required columns.\n" \
                "Expected:\n" \
                "- Barcode: '#{columns_hash["vonalkod-oszlop"]}'\n" \
                "- Quantity: '#{columns_hash["mennyiseg-oszlop"]}'\n" \
                "- Price: '#{columns_hash["eladasi-ar-oszlop"]}'\n" \
                "Found headers: #{header_row.inspect}"
            honama_log(error_message)
            raise "❗#{error_message}"
        end

        # Get the actual data rows by skipping the header row and the last row
        data_rows = csv_data[1...-1]

        data_from_file_hash = {}

        data_rows.each do |row|
            # Use the indexes we found instead of hardcoded numbers like row[0], row[1]
            barcode = (row[barcode_index] || '').to_s[/[A-Za-z0-9]+/]
            if barcode.nil?
				next
			end

			barcode = barcode.sub(/\.0+\z/, '')  # drop trailing ".0" from Excel numbers

            raw = row[quantity_index].to_s
            quantity = raw.strip.match?(/\A-\d+(\.\d+)?\z/) ? raw.to_f : raw.gsub(/[^\d\.]/, '').to_f
            quantity = quantity.to_i
            quantity = 0 if quantity < 0

            gross_price = row[price_index].to_s.gsub(/[^\d\.]/, '').to_f
            next if gross_price <= 0.0

            data_from_file_hash[barcode] ||= {
                "price"             => gross_price,
                "quantity"          => quantity
            }
        end

        data_from_file_hash
    end

    def prepare_data_to_update_from_file(data_from_file_hash, product_data_hash)
        update_product_hash = {}

        product_data_hash.each do |product_id, variant_id, barcode, location_id, inventory_item_id|
            # product is not in the excel file so skip it, we only want to update products that are in the file
            next unless data_from_file_hash.key?(barcode)

            update_product_hash[barcode] = {
                "status"            => "ACTIVE",
                "productId"         => product_id,
                "variantId"         => variant_id,
                "inventoryItemId"   => inventory_item_id,
                "locationId"        => location_id,
                "price"             => data_from_file_hash[barcode]["price"],
                "quantity"          => data_from_file_hash[barcode]["quantity"]
            }
        end

        @log.call("\nFound #{update_product_hash.size} items that need a price update.")

        return update_product_hash
    end

    # def is_throttled?(response)
    #   # Shopify's GraphQL API signals throttling in the "extensions" block of an error.
    #   return true if response.dig("errors", 0, "extensions", "code") == "DOCUMENT_TOKEN_LIMIT_EXCEEDED"
    #   return true if response.dig("errors", 0, "extensions", "code") == "THROTTLED"
    #   # As a fallback, check for common rate limit messages.
    #   return true if response.to_s.downcase.include?("exceeds the single query max cost limit")
    #   return true if response.to_s.downcase.include?("rate limit")
    #   return false
    # end

    # def run_cost_update(cost_updates_hash)
    #     # Update product cost values
    #     error_count = 0
    #     not_found = 0

    #     items_to_process = cost_updates_hash.to_a # Convert the hash to an array to be processed
    #     total_items = items_to_process.size

    #     current_batch_size = DEFAULT_BATCH_SIZE
    #     min_batch_size = 5 # Never go below a batch size of 1
    #     # Track the last successful step to calculate the increase
    #     last_step_down = 0

    #     highest_success = 1
    #     lowest_failure = DEFAULT_BATCH_SIZE    # Start with a very high value
    #     batch_size_locked = false

    #     # initial progress
    #     @progress&.call(0)

    #     # --- Main Processing Loop ---
    #     while !items_to_process.empty?

    #         if batch_size_locked
    #             current_batch_size = highest_success
    #         end

    #         # 1. Create a batch of the current dynamic size.
    #         batch_items = items_to_process.first(current_batch_size)
    #         batch_data = batch_items.to_h

    #         # 2. Call the update function for the current batch.
    #         variant_res, not_found_in_batch = update_cost_batch(batch_data)

    #         # 3. Analyze the response and adapt.
    #         if is_throttled?(variant_res)
    #             # --- ACTION: THROTTLED ---

    #             lowest_failure = current_batch_size
    #             # Halve the batch size and record the step size.
    #             new_size = (current_batch_size / 2).floor
    #             last_step_down = current_batch_size - new_size
    #             current_batch_size = [new_size, min_batch_size].max # Ensure it's at least the minimum

    #             sleep(1) # Wait for 2 seconds before retrying the same batch
    #             next # Go to the next loop iteration without removing items

    #         # Check for top-level syntax errors first
    #         elsif variant_res.key?("errors")
    #             top_level_errors = variant_res["errors"]
    #             message = "run_cost_update: API returned a top-level GraphQL error: #{top_level_errors}"
    #             honama_log(message)
    #             raise "❗️#{message}"

    #         elsif variant_res.key?("data")
    #             # --- ACTION: SUCCESS ---
    #             highest_success = [highest_success, current_batch_size].max
    #             # The batch was successful, so remove these items from the list.
    #             items_to_process.shift(batch_items.size)

    #             variant_res["data"].each do |barcode, result|
    #             # Process the successful response data (your existing logic).
    #                 if result["userErrors"] && !result["userErrors"].empty?
    #                     # Handle user errors for specific items in the batch
    #                     error_count += 1
    #                     message = "\nrun_cost_update: Found errors in '#{barcode}': #{user_errors}"
    #                     honama_log(message)
    #                     @log.call("#{message}")
    #                 else
    #                     # Update cache for successful items
    #                     product_variant = result["productVariants"][0]
	# 					barcode = product_variant["barcode"]
    #                     # update cache if update was successful for the given product
    #                     cache = ( JSON.parse( File.read( CACHE_FILE ))  rescue {} )

    #                     cache[barcode] ||= {}
    #                     cache[barcode]["cost"] = product_variant.dig("inventoryItem", "unitCost", "amount").to_f

    #                     File.write( CACHE_FILE, cache.to_json )
    #                 end
    #             end

    #             # --- CONVERGENCE LOGIC ---
    #             if lowest_failure == highest_success + 1
    #                 # We've found the exact limit! Lock it in.
    #                 batch_size_locked = true
    #             elsif !batch_size_locked
    #                 # Still searching. Let's probe halfway to our lowest known failure.
    #                 # If lowest_failure is still infinity, just probe by a fixed amount.
    #                 if lowest_failure == DEFAULT_BATCH_SIZE
    #                     current_batch_size = highest_success + 10 # Probe upwards
    #                 else
    #                     probe_step = ((lowest_failure - highest_success) / 2).ceil
    #                     current_batch_size = highest_success + probe_step
    #                 end
    #             end

    #         else
    #             # --- ACTION: HARD ERROR ---
    #             # This is not a throttle error, but another problem (e.g., GraphQL syntax).
    #             # Log the error and skip this batch to avoid an infinite loop.
    #             message = "run_cost_update: Unidentified error during batch update: #{variant_res}"
    #             @log.call(message)
    #             honama_log(message)
    #             error_count += batch_items.size
    #             items_to_process.shift(batch_items.size) # Skip the failed batch
    #         end

    #         # --- Progress Update ---
    #         processed_count = total_items - items_to_process.size
    #         percent = ((processed_count.to_f / total_items) * 100).round
    #         @progress&.call(percent)
    #     end

    #     @log.call("\n✅ Cost updates are done!")
    #     # ... (your final logging for errors)
    # end

    # def update_cost_batch(data)
    #     not_found = 0

    #     query = ""
    #     query << "mutation {\n"

    #     data.each{|barcode, details|
    #         if !details.key?("productId") || !details.key?("variantId")
    #             message = "update_cost_batch: Product ID or Variant ID was not found for barcode: '#{barcode}'"
    #             honama_log(message)
    #             @log.call("❗️#{message}")
    #             not_found += 1
    #             next
    #         end

    #         query << "  op_#{ barcode }: productVariantsBulkUpdate(\n"
    #         query << "    productId: \"#{details["productId"]}\",\n"
    #         query << "    variants: [ {\n"
    #         query << "      id: \"#{ details["variantId"] }\",\n"
    #         query << "      inventoryItem: {\n"
    #         query << "          cost: #{details["cost"]}\n"
    #         query << "          tracked: true\n"
    #         query << "      }\n"
    #         query << "      inventoryPolicy: DENY\n"
    #         query << "    } ],\n"
    #         query << "    allowPartialUpdates: true\n"
    #         query << "  ) {\n"
    #         query << "    productVariants {\n"
    #         query << "      id\n"
    #         query << "      barcode\n"
    #         query << "      inventoryItem {\n"
    #         query << "          unitCost {\n"
    #         query << "              amount\n"
    #         query << "          }\n"
    #         query << "      }\n"
    #         query << "    }\n"
    #         query << "    userErrors {\n"
    #         query << "      field\n"
    #         query << "      message\n"
    #         query << "    }\n"
    #         query << "  }\n"
    #     }
    #     query << "}\n"

    #     return honama_helper_shopify_api( SHOP, TOKEN, { query: query } ), not_found
    # end

    # def run_quantity_update(quantity_updates_hash)
    #     # Update product cost values
    #     error_count = 0
    #     not_found = 0

    #     items_to_process = quantity_updates_hash.to_a # Convert the hash to an array to be processed
    #     total_items = items_to_process.size

    #     current_batch_size = DEFAULT_BATCH_SIZE
    #     min_batch_size = 5 # Never go below a batch size of 1
    #     # Track the last successful step to calculate the increase
    #     last_step_down = 0

    #     highest_success = 1
    #     lowest_failure = DEFAULT_BATCH_SIZE    # Start with a very high value
    #     batch_size_locked = false

    #     # initial progress
    #     @progress&.call(0)

    #     # --- Main Processing Loop ---
    #     while !items_to_process.empty?

    #         if batch_size_locked
    #             current_batch_size = highest_success
    #         end

    #         # 1. Create a batch of the current dynamic size.
    #         batch_items = items_to_process.first(current_batch_size)
    #         batch_data = batch_items.to_h

    #         # Call the update function for the current batch
    #         result, not_found = update_quantity_batch(batch_data)

    #         # 3. Analyze the response and adapt.
    #         if is_throttled?(result)
    #             # --- ACTION: THROTTLED ---

    #             lowest_failure = current_batch_size
    #             # Halve the batch size and record the step size.
    #             new_size = (current_batch_size / 2).floor
    #             last_step_down = current_batch_size - new_size
    #             current_batch_size = [new_size, min_batch_size].max # Ensure it's at least the minimum

    #             sleep(1) # Wait for 2 seconds before retrying the same batch
    #             next # Go to the next loop iteration without removing items

    #         # Check for top-level syntax errors first
    #         elsif result.key?("errors")
    #             top_level_errors = result["errors"]
    #             message = "run_quantity_update: API returned a top-level GraphQL error: #{top_level_errors}"
    #             honama_log(message)
    #             raise "❗️#{message}"

    #         # Iterate through each update operation (update1, update2, etc.)
    #         elsif result.key?("data")
    #             # --- ACTION: SUCCESS ---
    #             highest_success = [highest_success, current_batch_size].max
    #             # The batch was successful, so remove these items from the list.
    #             items_to_process.shift(batch_items.size)

    #             result["data"].each do |barcode_res, result|
    #                 barcode = barcode_res.delete_prefix('op_')
    #                 user_errors = result["userErrors"]

    #                 # Check if the userErrors array is not empty
    #                 if user_errors && !user_errors.empty?
    #                     error_count += 1
    #                     message = "run_quantity_update: Found errors in '#{barcode}': #{user_errors}"
    #                     honama_log(message)
    #                     @log.call("\n#{message}")

	# 				# When updating product quantity qith the same number as it already is in webshops' database
    #                 elsif result["inventoryAdjustmentGroup"] == nil
	# 					# update cache if update was successful for the given product
    #                     cache = ( JSON.parse( File.read( CACHE_FILE ))  rescue {} )

    #                     cache[barcode] ||= {}
    #                     cache[barcode]["quantity"] = batch_data[barcode]["quantity"]

    #                     File.write( CACHE_FILE, cache.to_json )

	# 				else
    #                     inventory_changes = result["inventoryAdjustmentGroup"]["changes"][0]
    #                     barcode_from_res = inventory_changes.dig("item", "variant", "barcode")

    #                     # update cache if update was successful for the given product
    #                     cache = ( JSON.parse( File.read( CACHE_FILE ))  rescue {} )

    #                     cache[barcode_from_res] ||= {}
    #                     cache[barcode_from_res]["quantity"] = batch_data[barcode_from_res]["quantity"]

    #                     File.write( CACHE_FILE, cache.to_json )
    #                 end
    #             end

    #             # --- CONVERGENCE LOGIC ---
    #             if lowest_failure == highest_success + 1
    #                 # We've found the exact limit! Lock it in.
    #                 batch_size_locked = true
    #             elsif !batch_size_locked
    #                 # Still searching. Let's probe halfway to our lowest known failure.
    #                 # If lowest_failure is still infinity, just probe by a fixed amount.
    #                 if lowest_failure == DEFAULT_BATCH_SIZE
    #                     current_batch_size = highest_success + 10 # Probe upwards
    #                 else
    #                     probe_step = ((lowest_failure - highest_success) / 2).ceil
    #                     current_batch_size = highest_success + probe_step
    #                 end
    #             end
    #         else
    #             message = "run_quantity_update: Unidentified key during updating batch: #{quantity_update_res}"
    #             honama_log(message)
    #             raise "#{message}"
    #         end

    #         # --- Progress Update ---
    #         processed_count = total_items - items_to_process.size
    #         percent = ((processed_count.to_f / total_items) * 100).round
    #         @progress&.call(percent)
    #     end

    #     @log.call("\n✅ Quantity updates are done!")
    #     if not_found > 0
    #         @log.call("\nProducts not found during quantity update: #{not_found}")
    #     end
    #     if error_count > 0
    #         @log.call("\nErrors during quantity update: #{error_count}")
    #     end
    # end

    # def update_quantity_batch(data)
    #     not_found = 0

    #     query = ""
    #     query << "mutation {\n"

    #     data.each{|barcode, details|
    #         if !details.key?("productId") || !details.key?("variantId")
    #             message = "update_quantity_batch2: Product ID or Variant ID was not found for barcode: '#{barcode}'"
    #             honama_log(message)
    #             @log.call("❗️" + message)
    #             not_found += 1
    #             next
    #         end

    #         query << "  op_#{ barcode }: inventorySetQuantities(\n"
    #         query << "    input: {\n"
    #         query << "      ignoreCompareQuantity: true,\n"
    #         query << "      name: \"on_hand\",\n"
    #         query << "      reason: \"correction\",\n"
    #         query << "      quantities: [ {\n"
    #         query << "          compareQuantity: null,\n"
    #         query << "          inventoryItemId: \"#{details["inventoryItemId"]}\",\n"
    #         query << "          locationId: \"#{details["locationId"]}\",\n"
    #         query << "          quantity: #{details["quantity"]}\n"
    #         query << "      } ]\n"
    #         query << "    }\n"
    #         query << "  ) {\n"
    #         query << "      inventoryAdjustmentGroup {\n"
    #         query << "          changes {\n"
    #         query << "              item {\n"
    #         query << "                  variant {\n"
    #         query << "                      barcode\n"
    #         query << "                  }\n"
    #         query << "              }\n"
    #         query << "          }\n"
    #         query << "      }\n"
    #         query << "      userErrors {\n"
    #         query << "          field\n"
    #         query << "          message\n"
    #         query << "      }\n"
    #         query << "    }\n"
    #     }
    #     query << "}\n"

    #     return honama_helper_shopify_api( SHOP, TOKEN, { query: query } ), not_found
    # end

    # def run_status_update(status_updates_hash)
    #     # Update product cost values
    #     error_count = 0
    #     not_found = 0

    #     items_to_process = status_updates_hash.to_a # Convert the hash to an array to be processed
    #     total_items = items_to_process.size

    #     current_batch_size = DEFAULT_BATCH_SIZE
    #     min_batch_size = 5 # Never go below a batch size of 1
    #     # Track the last successful step to calculate the increase
    #     last_step_down = 0

    #     highest_success = 1
    #     lowest_failure = DEFAULT_BATCH_SIZE    # Start with a very high value
    #     batch_size_locked = false

    #     # initial progress
    #     @progress&.call(0)

    #     # --- Main Processing Loop ---
    #     while !items_to_process.empty?

    #         if batch_size_locked
    #             current_batch_size = highest_success
    #         end

    #         # 1. Create a batch of the current dynamic size.
    #         batch_items = items_to_process.first(current_batch_size)
    #         batch_data = batch_items.to_h

    #         # Call the update function for the current batch
    #         variant_res, not_found = update_status_batch(batch_data)

    #         # 3. Analyze the response and adapt.
    #         if is_throttled?(variant_res)
    #             # --- ACTION: THROTTLED ---

    #             lowest_failure = current_batch_size
    #             # Halve the batch size and record the step size.
    #             new_size = (current_batch_size / 2).floor
    #             last_step_down = current_batch_size - new_size
    #             current_batch_size = [new_size, min_batch_size].max # Ensure it's at least the minimum

    #             sleep(1) # Wait for 2 seconds before retrying the same batch
    #             next # Go to the next loop iteration without removing items

    #         # Check for top-level syntax errors first
    #         elsif variant_res.key?("errors")
    #             top_level_errors = variant_res["errors"]
    #             message = "run_cost_update: API returned a top-level GraphQL error: #{top_level_errors}"
    #             honama_log(message)
    #             raise "❗️#{message}"

    #         # Iterate through each update operation (update1, update2, etc.)
    #         elsif variant_res.key?("data")
    #             # --- ACTION: SUCCESS ---
    #             highest_success = [highest_success, current_batch_size].max
    #             # The batch was successful, so remove these items from the list.
    #             items_to_process.shift(batch_items.size)

    #             variant_res["data"].each do |barcode_res, result|
    #                 # Check if the userErrors array is not empty
    #                 if result["userErrors"] && !result["userErrors"].empty?
    #                     error_count += 1
    #                     message = "\nrun_status_update: Found errors in '#{barcode}': #{result["userErrors"]}"
    #                     honama_log(message)
    #                     @log.call("#{message}")
    #                 else
    #                     product = result["product"]
    #                     barcode = barcode_res.delete_prefix('op_')
    #                     # update cache if update was successful for the given product
    #                     cache = ( JSON.parse( File.read( CACHE_FILE ))  rescue {} )

    #                     cache[barcode] ||= {}
    #                     cache[barcode]["status"] = product["status"]

    #                     File.write( CACHE_FILE, cache.to_json )
    #                 end
    #             end

    #             # --- CONVERGENCE LOGIC ---
    #             if lowest_failure == highest_success + 1
    #                 # We've found the exact limit! Lock it in.
    #                 batch_size_locked = true
    #             elsif !batch_size_locked
    #                 # Still searching. Let's probe halfway to our lowest known failure.
    #                 # If lowest_failure is still infinity, just probe by a fixed amount.
    #                 if lowest_failure == DEFAULT_BATCH_SIZE
    #                     current_batch_size = highest_success + 10 # Probe upwards
    #                 else
    #                     probe_step = ((lowest_failure - highest_success) / 2).ceil
    #                     current_batch_size = highest_success + probe_step
    #                 end
    #             end
    #         else
    #             message = "run_status_update: Unidentified key during updating batch: #{variant_res}"
    #             honama_log(message)
    #             raise "#{message}"
    #         end

    #         # --- Progress Update ---
    #         processed_count = total_items - items_to_process.size
    #         percent = ((processed_count.to_f / total_items) * 100).round
    #         @progress&.call(percent)
    #     end

    #     @log.call("\n✅ Status updates are done!")
    #     if not_found > 0
    #         @log.call("\nProducts not found during status update: #{not_found}")
    #     end
    #     if error_count > 0
    #         @log.call("\nErrors during status update: #{error_count}")
    #     end
    # end

    # def update_status_batch(data)
    #     not_found = 0

    #     query = ""
    #     query << "mutation {\n"

    #     data.each{|barcode, details|
    #         if !details.key?("productId")
    #             message = "update_status_batch: Product ID was not found for barcode: '#{barcode}'"
    #             honama_log(message)
    #             @log.call("❗️#{message}")
    #             not_found += 1
    #             next
    #         end

    #         query << "  op_#{ barcode }: productUpdate(\n"
    #         query << "    product: { \n"
    #         query << "      id: \"#{details["productId"]}\",\n"
    #         query << "      status: #{details["status"]}\n"
    #         query << "    } \n"
    #         query << "  ) {\n"
    #         query << "    product {\n"
    #         query << "      id\n"
    #         query << "      status\n"
    #         query << "    }\n"
    #         query << "    userErrors {\n"
    #         query << "      field\n"
    #         query << "      message\n"
    #         query << "    }\n"
    #         query << "  }\n"
    #     }
    #     query << "}\n"

    #     return honama_helper_shopify_api( SHOP, TOKEN, { query: query } ), not_found
    # end

    # def run_fix_priced_update(fix_priced_updates)
    #     # Update product cost values
    #     error_count = 0
    #     not_found = 0

    #     items_to_process = fix_priced_updates.to_a # Convert the hash to an array to be processed
    #     total_items = items_to_process.size

    #     current_batch_size = DEFAULT_BATCH_SIZE
    #     min_batch_size = 5 # Never go below a batch size of 1
    #     # Track the last successful step to calculate the increase
    #     last_step_down = 0

    #     highest_success = 1
    #     lowest_failure = DEFAULT_BATCH_SIZE    # Start with a very high value
    #     batch_size_locked = false

    #     # initial progress
    #     @progress&.call(0)

    #     # --- Main Processing Loop ---
    #     while !items_to_process.empty?

    #         if batch_size_locked
    #             current_batch_size = highest_success
    #         end

    #         # 1. Create a batch of the current dynamic size.
    #         batch_items = items_to_process.first(current_batch_size)
    #         batch_data = batch_items.to_h

    #         # Call the update function for the current batch
    #         variant_res, not_found = update_fix_priced_batch(batch_data)

    #         # 3. Analyze the response and adapt.
    #         if is_throttled?(variant_res)
    #             # --- ACTION: THROTTLED ---

    #             lowest_failure = current_batch_size
    #             # Halve the batch size and record the step size.
    #             new_size = (current_batch_size / 2).floor
    #             last_step_down = current_batch_size - new_size
    #             current_batch_size = [new_size, min_batch_size].max # Ensure it's at least the minimum

    #             sleep(1) # Wait for 2 seconds before retrying the same batch
    #             next # Go to the next loop iteration without removing items

    #         # Check for top-level syntax errors first
    #         elsif variant_res.key?("errors")
    #             top_level_errors = variant_res["errors"]
    #             message = "run_fix_priced_update: API returned a top-level GraphQL error: #{top_level_errors}"
    #             honama_log(message)
    #             raise "❗️#{message}"

    #         # Iterate through each update operation (update1, update2, etc.)
    #         elsif variant_res.key?("data")
    #             # --- ACTION: SUCCESS ---
    #             highest_success = [highest_success, current_batch_size].max
    #             # The batch was successful, so remove these items from the list.
    #             items_to_process.shift(batch_items.size)

    #             variant_res["data"].each do |barcode, result|

    #                 # Check if the userErrors array is not empty
    #                 if result["userErrors"] && !result["userErrors"].empty?
    #                     error_count += 1
    #                     message = "\nrun_fix_priced_update: Found errors in '#{barcode}': #{result["userErrors"]}"
    #                     honama_log(message)
    #                     @log.call("#{message}")
    #                 else
    #                     product_variant = result["productVariants"][0]
	# 					barcode = product_variant["barcode"]
    #                     # update cache if update was successful for the given product
    #                     cache = ( JSON.parse( File.read( CACHE_FILE ))  rescue {} )

    #                     cache[barcode] ||= {}
    #                     cache[barcode]["price"] = product_variant["price"].to_f

    #                     File.write( CACHE_FILE, cache.to_json )
    #                 end
    #             end

    #             # --- CONVERGENCE LOGIC ---
    #             if lowest_failure == highest_success + 1
    #                 # We've found the exact limit! Lock it in.
    #                 batch_size_locked = true
    #             elsif !batch_size_locked
    #                 # Still searching. Let's probe halfway to our lowest known failure.
    #                 # If lowest_failure is still infinity, just probe by a fixed amount.
    #                 if lowest_failure == DEFAULT_BATCH_SIZE
    #                     current_batch_size = highest_success + 10 # Probe upwards
    #                 else
    #                     probe_step = ((lowest_failure - highest_success) / 2).ceil
    #                     current_batch_size = highest_success + probe_step
    #                 end
    #             end
    #         else
    #             message = "run_fix_priced_update: Unidentified key during updating batch: #{variant_res}"
    #             honama_log(message)
    #             raise "#{message}"
    #         end

    #         # --- Progress Update ---
    #         processed_count = total_items - items_to_process.size
    #         percent = ((processed_count.to_f / total_items) * 100).round
    #         @progress&.call(percent)
    #     end

    #     @log.call("\n✅ Price updates are done!")
    #     if not_found > 0
    #         @log.call("\nProducts not found during price update: #{not_found}")
    #     end
    #     if error_count > 0
    #         @log.call("\nErrors during price update: #{error_count}")
    #     end
    # end

    # def update_fix_priced_batch(data)
    #     not_found = 0

    #     query = ""
    #     query << "mutation {\n"

    #     data.each{|barcode, details|
    #         if !details.key?("productId") || !details.key?("variantId")
    #             message = "update_fix_priced_batch: Product ID or Variant ID was not found for barcode: '#{barcode}'"
    #             honama_log(message)
    #             @log.call("❗️#{message}")
    #             not_found += 1
    #             next
    #         end

    #         query << "  op_#{ barcode }: productVariantsBulkUpdate(\n"
    #         query << "    productId: \"#{details["productId"]}\",\n"
    #         query << "    variants: [ {\n"
    #         query << "      id: \"#{ details["variantId"] }\",\n"
    #         query << "      price: \"#{ details["price"] }\",\n"
    #         query << "    } ],\n"
    #         query << "    allowPartialUpdates: true\n"
    #         query << "  ) {\n"
    #         query << "    productVariants {\n"
    #         query << "      id\n"
    #         query << "      barcode\n"
    #         query << "      price\n"
    #         query << "    }\n"
    #         query << "    userErrors {\n"
    #         query << "      field\n"
    #         query << "      message\n"
    #         query << "    }\n"
    #         query << "  }\n"
    #     }
    #     query << "}\n"

    #     return honama_helper_shopify_api( SHOP, TOKEN, { query: query } ), not_found
    # end
end


#---------- GUI --------------------------------------------------------------------------------------------------------
# This class now includes the logic to disable buttons during the update process,
# additional input fields for cost adjustments, and functionality to save/load settings.
class ShopifyUpdater
    include Glimmer

    # --- Attributes ---
    attr_accessor :selected_file, :log_text, :update_running
    attr_accessor :select_file_enabled, :run_update_enabled
    attr_accessor :progress_value

    def initialize
        @selected_file = 'No file selected'
        @log_text = ''
        @update_running = false
        @full_filepath = nil

        # Initialize new cost parameters and load any saved settings
        @progress_value = 0

        @columns_hash = {}

        @supplier_independent_multiplier = 0

        @barcode_fix_priced_products_hash = {}

        # Initialize the button states
        update_button_states!

        # Register observers to automatically update button states whenever a source changes
        observe(self, :update_running) { update_button_states! }
        observe(self, :selected_file) { update_button_states! }

        create_gui
    end

    # This method centralizes the logic for determining button states
    def update_button_states!
        self.select_file_enabled = !self.update_running
        self.run_update_enabled = (!@full_filepath.nil? && @full_filepath != '' && !self.update_running)
    end

    def select_file
        filepath = open_file
        if filepath
            @full_filepath = filepath
            self.selected_file = File.basename(filepath)
        else
            # Handle case where user cancels file selection
            @full_filepath = nil
            self.selected_file = 'No file selected'
        end
    end

    def peek_headers(input_file)
        ext = File.extname(input_file).downcase
        if ['.xls', '.xlsx'].include?(ext)
            Spreadsheet.client_encoding = 'CP1250'
            sheet = Roo::Spreadsheet.open(input_file, extension: ext.delete('.').to_sym).sheet(0)
            sheet.row(1).map { |h| h.to_s.encode('UTF-8').strip }
        else
            # simple delimiter sniff
            first = File.open(input_file, &:readline)
            col_sep = [',', ';', "\t", '|'].max_by { |d| first.count(d) } || ','
            table = CSV.read(input_file, headers: true, col_sep: col_sep)
            table.headers.map { |h| h.to_s.strip }
        end
    end

    def run_update
        return unless @full_filepath

        # Run the main process in a separate thread so the GUI doesn't freeze
        Thread.new do
            # Set the update_running state to true, which triggers the observer to disable buttons
            Glimmer::LibUI.queue_main do
                self.update_running = true
                self.log_text = '' # Clear the log
                self.progress_value = 0
            end

            log = ->(message) { Glimmer::LibUI.queue_main { self.log_text += "#{message.force_encoding('UTF-8')}\n" } }
            progress = ->(percent) { Glimmer::LibUI.queue_main { self.progress_value = percent } }

            begin
                # --- Step 1: Load and Validate Configuration ---
                log.call("Loading configuration ...")
                success, config = load_and_validate_settings(log)
                return unless success # Stop if config failed to load

                # Define constants now that config is loaded
                Object.const_set('SHOP', config['SHOP']) unless defined?(::SHOP)
                Object.const_set('API_URL', config['API_URL']) unless defined?(::API_URL)
                Object.const_set('TOKEN', config['TOKEN']) unless defined?(::TOKEN)

                columns_hash = config["hasznalt-oszlopok"]

                log.call("✅ Configuration loaded successfully.")

                # Pass the cost adjustment values from the GUI to the logic handler
                logic_handler = UpdaterLogic.new(log, progress)

                # --- Step 3: Prepare and Validate Data from User's File ---
                log.call("\nPreparing data from '#{self.selected_file}'...")
                # This method will read the file and return the prepared data.
                # It logs its own errors and returns {} on failure.
                data_from_file_hash = logic_handler.prepare_data_from_file(@full_filepath, columns_hash)
                return if data_from_file_hash.empty? # Stop if file preparation failed
                log.call("✅ Data prepared successfully.")

                logic_handler.process_and_update_products(data_from_file_hash)

                progress.call(100)
                show_message_box("Success", "Cost and Quantity updates are done!")
            # Add a specific rescue for a common, user-fixable error
            rescue Errno::EACCES
                error_message = "❌ FILE LOCKED: '#{self.selected_file}' is open in another program.\nPlease close the file and try again."
                log.call("\n#{error_message}")
                show_message_box("Error", error_message)
            rescue => e
                error_message = "AN ERROR OCCURRED: #{e.message}\n#{e.backtrace}"
                log.call("\n❌ #{error_message}")
                show_message_box("Error", "Please send the error to us for analysis")
            ensure
                # Set the state back to false, which triggers the observer to re-enable buttons
                Glimmer::LibUI.queue_main { self.update_running = false }
            end
        end
    end

    def create_gui
        @main_window = window('Biocity Shopify Synchronizer', 900, 600) {
            margined true

            # Add the on_closing hook to save settings when the window is closed
            on_closing do
                exit(0)
            end

            vertical_box {
                padded true

                horizontal_box {
                    stretchy false
                    vertical_box {
                        stretchy false
                        label('File Selected:')
                    }
                    vertical_box {
                        label {
                            stretchy false
                            text <=> [self, :selected_file]
                        }
                    }
                }

                multiline_entry(readonly: true) {
                stretchy true
                text <=> [self, :log_text]
                }

                progress_bar {
                    stretchy false
                    # Value 0..100
                    value <=> [self, :progress_value]
                }

                horizontal_box {
                stretchy false
                label { stretchy true }

                button('Select File') {
                    stretchy false
                    enabled <=> [self, :select_file_enabled]
                    on_clicked { select_file }
                }

                label { stretchy true }

                button('Run Update') {
                    stretchy false
                    enabled <=> [self, :run_update_enabled]
                    on_clicked { run_update }
                }

                label { stretchy true }
                }
            }
        }
        @main_window.show
    end

    def show_message_box(title, message)
        Glimmer::LibUI.queue_main do
            msg_box = window(title, 300, 100) {
                margined true
                vertical_box {
                label(message) { stretchy false }
                    button('Close') {
                        stretchy false
                        on_clicked { msg_box.destroy }
                    }
                }
            }
            msg_box.show
        end
    end

    private

    # This method now returns a success status and the config data.
    def load_and_validate_settings(log)
        config_json_file = 'config.json'
        unless File.exist?(config_json_file)
            error_message = "❌ CONFIGURATION ERROR: '#{config_json_file}' not found. Please create it and try again."
            honama_log(error_message)
            log.call(error_message)
            return [false, nil] # Return failure
        end

        begin
            config = JSON.parse(File.read(config_json_file))

            if  !config.key?("SHOP") ||
                !config.key?("API_URL") ||
                !config.key?("TOKEN") ||
                !config.key?("BARCODE_FILE")

                error_message = "❌ CONFIGURATION ERROR: 'There are missing keys from configuration file!"
                honama_log(error_message)
                log.call(error_message)
                return [false, nil]
            end

            return [true, config] # Return success and the loaded config

        rescue JSON::ParserError => e
            error_message = "❌ CONFIGURATION ERROR: Could not parse '#{config_json_file}'. Please check for syntax errors.\nDetails: #{e.message}"
            honama_log(error_message)
            log.call(error_message)
            return [false, nil] # Return failure
        rescue => e
            error_message = "❌ CONFIGURATION ERROR: An unexpected error occurred while reading '#{config_json_file}'.\nDetails: #{e.message}"
            honama_log(error_message)
            log.call(error_message)
            return [false, nil] # Return failure
        end
    end

    def load_barcodes_of_fix_priced_products(log)
        barcode_fix_priced_products_hash = {}
        unless File.exist?(BARCODE_FILE)
            error_message = "❌ ERROR: '#{BARCODE_FILE}' not found. Please create it and try again."
            honama_log(error_message)
            log.call(error_message)
            return [false, nil] # Return failure
        end

        begin
            # 1. Read all non-empty lines from the file into an array.
            all_potential_keys = File.readlines(BARCODE_FILE, chomp: true).reject(&:empty?)

            # 2. Partition the array into two lists: valid and invalid.
            valid_keys, invalid_keys = all_potential_keys.partition do |key|
                key.match?(/\A[A-Za-z0-9]+\z/)
            end

            # 3. If there are any invalid keys, log a warning to the GUI.
            unless invalid_keys.empty?
                error_message = "load_settings: Warning: Found #{invalid_keys.length} invalid barcodes in '#{BARCODE_FILE}'."
                honama_log(error_message)
                log.call(error_message)
                return [false, nil]
            end

            # 4. Build the hash from ONLY the clean, validated array of keys.
            return [true, valid_keys.each_with_index.to_h]

        rescue => e
            error_message = "load_settings: An unexpected error occurred while reading '#{BARCODE_FILE}'.\nDetails: #{e.message}\n#{e.backtrace}"
            honama_log(error_message)
            log.call(error_message)
            return [false, nil]
        end
    end
end

# Create and launch the application
ShopifyUpdater.new
