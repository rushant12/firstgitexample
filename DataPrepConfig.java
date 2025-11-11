package com.broadridge.pdf2print.dataprep.modifyafp.models;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.broadridge.pdf2print.dataprep.modifyafp.MyCmdLine;
import com.broadridge.pdf2print.dataprep.modifyafp.utils.MyMessenger;
import com.dst.output.custsys.lib.jolt.StatementCustomException;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import lombok.Data;

@Data
public class DataPrepConfig {
	private static MyMessenger msger = MyMessenger.get();

	private static DataPrepConfig config = null;

	private String INSERT_BNG_ENG_RECS;
	private List<Map<String, String>> TRANSLATE_IMM_RECS;
	private List<Map<String, String>> SET_IMM_RECS;
	private List<Map<String, Object>> DELETE_TEXT;
	private List<String> INSERT_FILE_LEVEL_NOPS;
	private List<Map<String, String>> INSERT_FILE_LEVEL_NAMED_NOPS;
	private List<Map<String, String>> INSERT_FILE_HEADER_NAMED_NOPS;
	private List<Map<String, String>> INSERT_FILE_TRAILER_NAMED_NOPS;
	private List<Map<String, String>> INSERT_MAIL_PIECE_LEVEL_TLES;
	private List<Map<String, String>> INSERT_MAIL_PIECE_TLES;
	private List<Map<String, String>> INSERT_MAIL_PIECE_LEVEL_NAMED_NOPS;
	private List<Map<String, String>> INSERT_MAIL_PIECE_NAMED_NOPS;
	private List<Map<String, Object>> INSERT_OVERLAYS;
	private List<Map<String, Object>> INSERT_PSEGS;
	private Map<String, Object> SCRAPE_SEND_ADDRESS;
	private Map<String, Object> SCRAPE_AND_MOVE_SEND_ADDRESS;
	private List<Map<String, JsonElement>> INSERT_NEW_PAGES;
	private Map<String, String> CDFS;
	private Map<String, String> CHECK_IMM_TABLE;
	private Map<String, Integer> INSERT_ID_HOPPER_LOOKUP;
	private String EACH_PDF_DOCUMENT_STARTS_NEW_SHEET;
	private List<Map<String, Object>> SHIPPING_SH_CODE_LOOKUP;
	private List<Map<String, Object>> ADD_TEXT_TO_PAGES;
	private Map<String, Map<String, Object>> CREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS;
	private String INSERT_BLANK_PAGE_AFTER_EACH_INPUT_PAGE;

	private DataPrepConfig() {
	}

	public static DataPrepConfig get() throws IOException, JSONException {
		if (config == null) {
			config = new DataPrepConfig();
			String configFile = MyCmdLine.get().getConfigFile();
			String jsonString = new String(Files.readAllBytes(Paths.get(configFile)));
			JSONObject masterDataprepJsonObject = new JSONObject(jsonString);

			config.setINSERT_BNG_ENG_RECS(masterDataprepJsonObject.optString("INSERT_BNG_ENG_RECS"));

			// Setting TRANSLATE_IMM_RECS (Array of objects)
			JSONArray translateImmRecs = masterDataprepJsonObject.optJSONArray("TRANSLATE_IMM_RECS");
			List<Map<String, String>> translateList = new ArrayList<>();
			if (translateImmRecs != null) {
				for (int i = 0; i < translateImmRecs.length(); i++) {
					JSONObject entry = translateImmRecs.optJSONObject(i);
					if (entry != null) {
						Iterator<String> keys = entry.keys();
						Map<String, String> translateMap = new HashMap<>();
						while (keys.hasNext()) {
							String key = keys.next();
							String value = entry.optString(key);
							translateMap.put(key, value);
						}
						translateList.add(translateMap);
					}
				}
			}
			config.setTRANSLATE_IMM_RECS(translateList);

			// Setting SET_IMM_RECS (Array of objects)
			JSONArray setImmRecs = masterDataprepJsonObject.optJSONArray("SET_IMM_RECS");
			List<Map<String, String>> setList = new ArrayList<>();
			if (setImmRecs != null) {
				for (int i = 0; i < setImmRecs.length(); i++) {
					JSONObject entry = setImmRecs.optJSONObject(i);
					if (entry != null) {
						Iterator<String> keys = entry.keys();
						Map<String, String> immMap = new HashMap<>();
						while (keys.hasNext()) {
							String key = keys.next();
							String value = entry.optString(key);
							immMap.put(key, value);
						}
						setList.add(immMap);
					}
				}
			}
			config.setSET_IMM_RECS(setList);

			// Setting DELETE_TEXT 
			JSONArray deleteText = masterDataprepJsonObject.optJSONArray("DELETE_TEXT");
			List<Map<String, Object>> deleteList = new ArrayList<>();

			if (deleteText != null) {
			    for (int i = 0; i < deleteText.length(); i++) {
			        JSONObject obj = deleteText.optJSONObject(i);
			        if (obj != null) {
			            checkMandatoryFields(obj,
			                    Arrays.asList("page_num", "x1", "y1", "x2", "y2", "orientation"),
			                    "DELETE_TEXT at index " + i);

			            Map<String, Object> map = new HashMap<>();
			            map.put("page_num", obj.optInt("page_num"));
			            map.put("x1", obj.optDouble("x1"));
			            map.put("y1", obj.optDouble("y1"));
			            map.put("x2", obj.optDouble("x2"));
			            map.put("y2", obj.optDouble("y2"));
			            map.put("orientation", obj.optInt("orientation"));

			            deleteList.add(map);
			        }
			    }
			}

			config.setDELETE_TEXT(deleteList);


			// Setting INSERT_FILE_LEVEL_NOPS (Array of strings)
			JSONArray insertFileLevelNops = masterDataprepJsonObject.optJSONArray("INSERT_FILE_LEVEL_NOPS");
			List<String> nopsList = new ArrayList<>();
			if (insertFileLevelNops != null) {
				for (int i = 0; i < insertFileLevelNops.length(); i++) {
					String value = insertFileLevelNops.optString(i);
					nopsList.add(value);
				}
			}
			config.setINSERT_FILE_LEVEL_NOPS(nopsList);

			JSONArray insertFileHeaderNamedNops = masterDataprepJsonObject
					.optJSONArray("INSERT_FILE_HEADER_NAMED_NOPS");
			List<Map<String, String>> nopsHeaderList = new ArrayList<>();
			if (insertFileHeaderNamedNops != null) {
				for (int i = 0; i < insertFileHeaderNamedNops.length(); i++) {
					JSONObject entry = insertFileHeaderNamedNops.optJSONObject(i);
					if (entry != null) {
						Iterator<String> keys = entry.keys();
						Map<String, String> namedNopMap = new HashMap<>();
						while (keys.hasNext()) {
							String key = keys.next();
							String value = entry.optString(key);
							namedNopMap.put(key, value);
						}
						nopsHeaderList.add(namedNopMap);
					}
				}
			}
			config.setINSERT_FILE_HEADER_NAMED_NOPS(nopsHeaderList);

			JSONArray insertFileTrailerNamedNops = masterDataprepJsonObject
					.optJSONArray("INSERT_FILE_TRAILER_NAMED_NOPS");
			List<Map<String, String>> nopsTrailerList = new ArrayList<>();
			if (insertFileTrailerNamedNops != null) {
				for (int i = 0; i < insertFileTrailerNamedNops.length(); i++) {
					JSONObject entry = insertFileTrailerNamedNops.optJSONObject(i);
					if (entry != null) {
						Iterator<String> keys = entry.keys();
						Map<String, String> namedNopMap = new HashMap<>();
						while (keys.hasNext()) {
							String key = keys.next();
							String value = entry.optString(key);
							namedNopMap.put(key, value);
						}
						nopsTrailerList.add(namedNopMap);
					}
				}
			}
			config.setINSERT_FILE_TRAILER_NAMED_NOPS(nopsTrailerList);

			JSONArray insertMailpieceTles = masterDataprepJsonObject.optJSONArray("INSERT_MAIL_PIECE_TLES");
			List<Map<String, String>> mailPieceTleList = new ArrayList<>();
			if (insertMailpieceTles != null) {
				for (int i = 0; i < insertMailpieceTles.length(); i++) {
					JSONObject entry = insertMailpieceTles.optJSONObject(i);
					if (entry != null) {
						Iterator<String> keys = entry.keys();
						Map<String, String> namedNopMap = new HashMap<>();
						while (keys.hasNext()) {
							String key = keys.next();
							String value = entry.optString(key);
							namedNopMap.put(key, value);
						}
						mailPieceTleList.add(namedNopMap);
					}
				}
			}
			config.setINSERT_MAIL_PIECE_TLES(mailPieceTleList);

			JSONArray insertMailpieceNops = masterDataprepJsonObject.optJSONArray("INSERT_MAIL_PIECE_NAMED_NOPS");
			List<Map<String, String>> mailPieceNopList = new ArrayList<>();
			if (insertMailpieceNops != null) {
				for (int i = 0; i < insertMailpieceNops.length(); i++) {
					JSONObject entry = insertMailpieceNops.optJSONObject(i);
					if (entry != null) {
						Iterator<String> keys = entry.keys();
						Map<String, String> namedNopMap = new HashMap<>();
						while (keys.hasNext()) {
							String key = keys.next();
							String value = entry.optString(key);
							namedNopMap.put(key, value);
						}
						mailPieceNopList.add(namedNopMap);
					}
				}
			}
			config.setINSERT_MAIL_PIECE_NAMED_NOPS(mailPieceNopList);

			// Setting INSERT_FILE_LEVEL_NAMED_NOPS (Array of objects)
			JSONArray insertFileLevelNamedNops = masterDataprepJsonObject.optJSONArray("INSERT_FILE_LEVEL_NAMED_NOPS");
			List<Map<String, String>> namedNopsList = new ArrayList<>();
			if (insertFileLevelNamedNops != null) {
				for (int i = 0; i < insertFileLevelNamedNops.length(); i++) {
					JSONObject entry = insertFileLevelNamedNops.optJSONObject(i);
					if (entry != null) {
						Iterator<String> keys = entry.keys();
						Map<String, String> nopMap = new HashMap<>();
						while (keys.hasNext()) {
							String key = keys.next();
							String value = entry.optString(key);
							nopMap.put(key, value);
						}
						namedNopsList.add(nopMap);
					}
				}
			}
			config.setINSERT_FILE_LEVEL_NAMED_NOPS(namedNopsList);

			// Setting SCRAPE_SEND_ADDRESS 
			JSONObject scrapeSendAddress = masterDataprepJsonObject.optJSONObject("SCRAPE_SEND_ADDRESS");

			if (scrapeSendAddress != null) {
			    checkMandatoryFields(scrapeSendAddress,
			            Arrays.asList("x1", "y1", "x2", "y2", "orientation"),
			            "SCRAPE_SEND_ADDRESS");

			    Map<String, Object> scrapeSendMap = new HashMap<>();
			    scrapeSendMap.put("x1", scrapeSendAddress.optDouble("x1"));
			    scrapeSendMap.put("y1", scrapeSendAddress.optDouble("y1"));
			    scrapeSendMap.put("x2", scrapeSendAddress.optDouble("x2"));
			    scrapeSendMap.put("y2", scrapeSendAddress.optDouble("y2"));
			    scrapeSendMap.put("orientation", scrapeSendAddress.optInt("orientation"));

			    if (scrapeSendAddress.has("foreign_sh_code")) {
			        scrapeSendMap.put("foreign_sh_code", scrapeSendAddress.optString("foreign_sh_code"));
			    }

			    config.setSCRAPE_SEND_ADDRESS(scrapeSendMap);
			}


			// Setting SCRAPE_AND_MOVE_SEND_ADDRESS 
			JSONObject scrapeAndMoveSendAddress = masterDataprepJsonObject
			        .optJSONObject("SCRAPE_AND_MOVE_SEND_ADDRESS");

			if (scrapeAndMoveSendAddress != null) {
			    Map<String, Object> scrapeAndMoveSendMap = new HashMap<>();

			    // FROM section
			    JSONObject from = scrapeAndMoveSendAddress.optJSONObject("from");
			    if (from != null) {
			        checkMandatoryFields(from, Arrays.asList("x1", "y1", "x2", "y2", "orientation"),
			                "SCRAPE_AND_MOVE_SEND_ADDRESS.from");

			        Map<String, Object> fromMap = new HashMap<>();
			        fromMap.put("x1", from.optDouble("x1"));
			        fromMap.put("y1", from.optDouble("y1"));
			        fromMap.put("x2", from.optDouble("x2"));
			        fromMap.put("y2", from.optDouble("y2"));
			        fromMap.put("orientation", from.optDouble("orientation"));

			        scrapeAndMoveSendMap.put("from", fromMap);
			    }

			    // TO section
			    JSONObject to = scrapeAndMoveSendAddress.optJSONObject("to");
			    if (to != null) {
			        checkMandatoryFields(to, Arrays.asList("x1", "y1", "orientation"),
			                "SCRAPE_AND_MOVE_SEND_ADDRESS.to");

			        Map<String, Object> toMap = new HashMap<>();
			        toMap.put("x1", to.optDouble("x1"));
			        toMap.put("y1", to.optDouble("y1"));
			        toMap.put("orientation", to.optInt("orientation"));

			        if (isNotBlank(to.optString("new_font"))) {
			            toMap.put("new_font", to.optJSONObject("new_font"));
			        }
			        if (to.has("line_spacing")) {
			            toMap.put("line_spacing", to.optDouble("line_spacing"));
			        }

			        scrapeAndMoveSendMap.put("to", toMap);
			    }

			    config.setSCRAPE_AND_MOVE_SEND_ADDRESS(scrapeAndMoveSendMap);
			}


			// insert new pages
			Gson gson = new Gson();
			JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
			JsonArray insertNewPages = (JsonArray) jsonObject.get("INSERT_NEW_PAGES");
			List<Map<String, JsonElement>> newPageList = new ArrayList<>();
			if (insertNewPages != null && insertNewPages.size() > 0) {
				for (int i = 0; i < insertNewPages.size(); i++) {
					JsonObject ob = (JsonObject) insertNewPages.get(i);
					newPageList.add(ob.asMap());
				}
				config.setINSERT_NEW_PAGES(newPageList);
			}

			// Setting INSERT_OVERLAYS
			JSONArray overlays = masterDataprepJsonObject.optJSONArray("INSERT_OVERLAYS");
			List<Map<String, Object>> overlayList = new ArrayList<>();

			if (overlays != null) {
			    for (int i = 0; i < overlays.length(); i++) {
			        JSONObject obj = overlays.optJSONObject(i);
			        if (obj != null) {
			            checkMandatoryFields(obj, Arrays.asList("page_num", "x_offset", "y_offset", "overlay_name"),
			                    "INSERT_OVERLAYS at index " + i);

			            Map<String, Object> map = new HashMap<>();
			            map.put("page_num", obj.optInt("page_num"));
			            map.put("x_offset", obj.optDouble("x_offset"));
			            map.put("y_offset", obj.optDouble("y_offset"));
			            map.put("overlay_name", obj.optString("overlay_name"));
			            overlayList.add(map);
			        }
			    }
			}

			config.setINSERT_OVERLAYS(overlayList);


			JSONArray hopperLookupArray = masterDataprepJsonObject.optJSONArray("INSERT_ID_HOPPER_LOOKUP");
			if (hopperLookupArray != null) {
				Map<String, Integer> hopperLookupMap = new HashMap<>();
				for (int i = 0; i < hopperLookupArray.length(); i++) {
					JSONObject obj = hopperLookupArray.getJSONObject(i);
					Iterator<String> keys = obj.keys();
					while (keys.hasNext()) {
						String key = keys.next();
						int value = obj.optInt(key);
						hopperLookupMap.put(key, value);
					}
				}
				config.setINSERT_ID_HOPPER_LOOKUP(hopperLookupMap);
			}

			config.setEACH_PDF_DOCUMENT_STARTS_NEW_SHEET(
					masterDataprepJsonObject.optString("EACH_PDF_DOCUMENT_STARTS_NEW_SHEET"));
			
			// Add Blank
			config.setINSERT_BLANK_PAGE_AFTER_EACH_INPUT_PAGE(
			        masterDataprepJsonObject.optString("INSERT_BLANK_PAGE_AFTER_EACH_INPUT_PAGE"));

			// Finish setting the values
			
			// SHIPPING_SH_CODE_LOOKUP setting
			JSONArray shippingShCodeLookupArray = masterDataprepJsonObject.optJSONArray("SHIPPING_SH_CODE_LOOKUP");
			List<Map<String, Object>> shippingShCodeLookupList = new ArrayList<>();

			if (shippingShCodeLookupArray != null) {
			    for (int i = 0; i < shippingShCodeLookupArray.length(); i++) {
			        JSONObject entry = shippingShCodeLookupArray.optJSONObject(i);
			        if (entry != null) {
			            checkMandatoryFields(entry, Arrays.asList("carrier", "service", "sh_code"),
			                    "SHIPPING_SH_CODE_LOOKUP at index " + i);

			            Map<String, Object> map = new HashMap<>();
			            map.put("carrier", entry.optString("carrier").toLowerCase());
			            map.put("service", entry.optString("service").toLowerCase());
			            map.put("sh_code", entry.optInt("sh_code"));
			            shippingShCodeLookupList.add(map);
			        }
			    }
			}
			config.setSHIPPING_SH_CODE_LOOKUP(shippingShCodeLookupList);


			// Add Text
			JSONArray addTextToPagesArray = masterDataprepJsonObject.optJSONArray("ADD_TEXT_TO_PAGES");
			List<Map<String, Object>> addTextToPagesList = new ArrayList<>();

			if (addTextToPagesArray != null) {
			    for (int i = 0; i < addTextToPagesArray.length(); i++) {
			        JSONObject pageEntry = addTextToPagesArray.optJSONObject(i);
			        if (pageEntry != null) {
			            checkMandatoryFields(pageEntry, Arrays.asList("page_num", "bpt_rec", "ept_rec"),
			                    "ADD_TEXT_TO_PAGES at index " + i);

			            Map<String, Object> pageMap = new HashMap<>();
			            pageMap.put("page_num", pageEntry.optInt("page_num"));
			            pageMap.put("bpt_rec", pageEntry.optString("bpt_rec"));
			            pageMap.put("ept_rec", pageEntry.optString("ept_rec"));

			            JSONArray mcfArray = pageEntry.optJSONArray("mcf_recs");
			            if (mcfArray != null) {
			                List<Map<String, Object>> mcfRecs = new ArrayList<>();
			                for (int j = 0; j < mcfArray.length(); j++) {
			                    JSONObject mcfObj = mcfArray.optJSONObject(j);
			                    if (mcfObj != null) {
			                        checkMandatoryFields(mcfObj,
			                                Arrays.asList("id", "coded_font_name", "character_set", "code_page"),
			                                "mcf_recs[" + j + "] at ADD_TEXT_TO_PAGES index " + i);

			                        Map<String, Object> mcfMap = new HashMap<>();
			                        mcfMap.put("id", mcfObj.getInt("id"));
			                        mcfMap.put("coded_font_name", mcfObj.optString("coded_font_name"));
			                        mcfMap.put("character_set", mcfObj.optString("character_set"));
			                        mcfMap.put("code_page", mcfObj.optString("code_page"));
			                        mcfMap.put("font_description", mcfObj.optString("font_description", ""));
			                        mcfRecs.add(mcfMap);
			                    }
			                }
			                pageMap.put("mcf_recs", mcfRecs);
			            }

			            JSONArray ptxArray = pageEntry.optJSONArray("ptx_recs");
			            if (ptxArray != null) {
			                List<Map<String, Object>> ptxRecs = new ArrayList<>();
			                for (int j = 0; j < ptxArray.length(); j++) {
			                    JSONObject ptxObj = ptxArray.optJSONObject(j);
			                    if (ptxObj != null) {
			                        checkMandatoryFields(ptxObj,
			                                Arrays.asList("x_offset", "y_offset", "orientation", "font_id", "trn_text"),
			                                "ptx_recs[" + j + "] at ADD_TEXT_TO_PAGES index " + i);

			                        Map<String, Object> ptxMap = new HashMap<>();
			                        ptxMap.put("x_offset", ptxObj.optDouble("x_offset"));
			                        ptxMap.put("y_offset", ptxObj.optDouble("y_offset"));
			                        ptxMap.put("orientation", ptxObj.optInt("orientation"));
			                        ptxMap.put("font_id", ptxObj.optInt("font_id"));
			                        ptxMap.put("trn_text", ptxObj.optString("trn_text"));
			                        ptxRecs.add(ptxMap);
			                    }
			                }
			                pageMap.put("ptx_recs", ptxRecs);
			            }

			            addTextToPagesList.add(pageMap);
			        }
			    }
			}

			config.setADD_TEXT_TO_PAGES(addTextToPagesList);


			// IPS
			JSONArray insertPsegs = masterDataprepJsonObject.optJSONArray("INSERT_PSEGS");
			List<Map<String, Object>> insertPsegsList = new ArrayList<>();
			if (insertPsegs != null) {
				List<String> mandatoryFields = Arrays.asList("page_num", "x_offset", "y_offset", "pseg_name");
				for (int i = 0; i < insertPsegs.length(); i++) {
					JSONObject entry = insertPsegs.optJSONObject(i);
					if (entry != null) {
						checkMandatoryFields(entry, mandatoryFields, "INSERT_PSEGS at index " + i);
						Map<String, Object> psegMap = new HashMap<>();
						psegMap.put("page_num", entry.optInt("page_num"));
						psegMap.put("pseg_name", entry.optString("pseg_name"));
						psegMap.put("x_offset", entry.optDouble("x_offset"));
						psegMap.put("y_offset", entry.optDouble("y_offset"));

						insertPsegsList.add(psegMap);
					}
				}
			}
			config.setINSERT_PSEGS(insertPsegsList);

			try {
				JSONObject cdfsJson = masterDataprepJsonObject.getJSONObject("CREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS");
				String cdfsJsonString = cdfsJson.toString();
				Gson gsonConvert = new Gson();
				List<String> cdfKeys = new ArrayList<>();
				Iterator<String> keyIterator = (Iterator<String>) cdfsJson.keys();
				keyIterator.forEachRemaining(key -> {
					if (key.startsWith("cdf_")) {
						cdfKeys.add(key);
					}
				});
				// Validate max 4
				if (cdfKeys.size() > 4) {
					throw new RuntimeException(
							new StatementCustomException("Maximum of 4 CDF fields allowed. Found: " + cdfKeys.size()));
				}
				// Define the type of the map
				Type type = new TypeToken<Map<String, Map<String, Object>>>() {
				}.getType();
				// Parse the JSON string into the desired type
				Map<String, Map<String, Object>> resultMap = gsonConvert.fromJson(cdfsJsonString, type);
				config.setCREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS(resultMap);
			} catch (JSONException e) {
				msger.logDebug("Key 'CREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS' not found or not a JSONObject.");
			}

		}
		return config;
	}

	private static void checkMandatoryFields(JSONObject obj, List<String> fields, String context) {
		for (String field : fields) {
			if (!obj.has(field)) {
				msger.printMsg("FATAL", "Missing mandatory field '" + field + "' in " + context);
			}
		}
	}

}
