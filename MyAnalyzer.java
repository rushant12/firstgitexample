package com.broadridge.pdf2print.dataprep.modifyafp;

import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.addCmdIPO;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.addCmdIPS;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.addCmdMCF;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.convertMapToJson;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.createIPORec;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.createPTXRecord;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.getJSONStringValue;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.inchToDP;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.numToDP;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils.populateAddressMap;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.Constants.ACCOUNT_NUMBER;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.Constants.PAGE_NUM;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.Constants.SEND_ADDRESS_LINES;
import static com.broadridge.pdf2print.dataprep.modifyafp.utils.Constants.SPECIAL_HANDLING_CODE;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.broadridge.pdf2print.dataprep.modifyafp.models.DataPrepConfig;
import com.broadridge.pdf2print.dataprep.modifyafp.models.FileCoordinate;
import com.broadridge.pdf2print.dataprep.modifyafp.models.MyDerivedData;
import com.broadridge.pdf2print.dataprep.modifyafp.models.MyRawData;
import com.broadridge.pdf2print.dataprep.modifyafp.models.Statement;
import com.broadridge.pdf2print.dataprep.modifyafp.utils.CommonUtils;
import com.broadridge.pdf2print.dataprep.modifyafp.utils.MetadataReader;
import com.broadridge.pdf2print.dataprep.modifyafp.utils.MyMessenger;
import com.broadridge.pdf2print.dataprep.modifyafp.utils.PerformanceMap;
import com.dst.output.custsys.lib.jolt.Analyzer;
import com.dst.output.custsys.lib.jolt.AnalyzerAdapter;
import com.dst.output.custsys.lib.jolt.DerivedData;
import com.dst.output.custsys.lib.jolt.Messenger;
import com.dst.output.custsys.lib.jolt.MultiFmtr;
import com.dst.output.custsys.lib.jolt.RawData;
import com.dst.output.custsys.lib.jolt.StatementCustomException;
import com.dst.output.custsys.lib.jolt.StatementFormatException;
import com.dst.output.custsys.lib.jolt.StatementNonFatalException;
import com.dst.output.custsys.lib.jolt.StatementSchemaException;
import com.dstoutput.custsys.jafp.AfpByteArrayList;
import com.dstoutput.custsys.jafp.AfpCmd;
import com.dstoutput.custsys.jafp.AfpCmdBAG;
import com.dstoutput.custsys.jafp.AfpCmdBPG;
import com.dstoutput.custsys.jafp.AfpCmdBPT;
import com.dstoutput.custsys.jafp.AfpCmdEAG;
import com.dstoutput.custsys.jafp.AfpCmdEPG;
import com.dstoutput.custsys.jafp.AfpCmdEPT;
import com.dstoutput.custsys.jafp.AfpCmdIMM;
import com.dstoutput.custsys.jafp.AfpCmdIPS;
import com.dstoutput.custsys.jafp.AfpCmdPGD;
import com.dstoutput.custsys.jafp.AfpCmdPTD;
import com.dstoutput.custsys.jafp.AfpCmdRaw;
import com.dstoutput.custsys.jafp.AfpCmdTLE;
import com.dstoutput.custsys.jafp.AfpCmdType;
import com.dstoutput.custsys.jafp.AfpNopGenericZD;
import com.dstoutput.custsys.jafp.AfpRec;
import com.dstoutput.custsys.jafp.AfpSFIntro;
import com.dstoutput.custsys.jafp.SBIN3;
import com.dstoutput.custsys.jafp.UBIN1;
import com.dstoutput.custsys.jafp.UBIN2;
import com.dstoutput.custsys.jafp.UBIN3;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class MyAnalyzer extends AnalyzerAdapter implements Analyzer {
	private static MyMessenger msger = MyMessenger.get();
	MultiFmtr multiFmtr;
	private final TextSpecification textSpecification = new TextSpecification();
	public static Map<String, String> tleMapPerStatement = null;
	String insertCombo;
	private MyCmdLine cmdLine = MyCmdLine.get();
	List<Integer> engRecIndexList = new ArrayList<>();
	Map<String, AfpRec> afpMap = new HashMap<String, AfpRec>();
	public JSONObject masterDataprepJsonObject;
	private String docAccountNumber = "";
	private Map<String, String> immSettings = new HashMap<>();
	private List<AfpRec> immRecords = new ArrayList<>();
	private Map<String, String> pSegsSettings = new HashMap<>();
	private List<AfpRec> pSegsRecords = new ArrayList<>();
	Map<String, AfpRec> afpPsegMap = new HashMap<String, AfpRec>();
	double pageDPI = 0.0;
	List<FileCoordinate> fileCoordinates = new ArrayList<>();
	private Map<String, String> addressLinesMap = new HashMap<String, String>();
	private static final Pattern SEND_ADDRESS_LINE_PATTERN = Pattern
			.compile("FROM_METADATA\\^send_address_line_(\\d)\\^");
	Map<String, List<List<AfpRec>>> afpPTXMap = new HashMap<String, List<List<AfpRec>>>();
	private boolean docAccountNumberAsASeprateTle = true;

	enum NewPageRecs {
		IMM_REC, BPT_REC, EPT_REC, BPG_REC, BAG_REC, EAG_REC, PGD_REC, PTD_REC, MCF_RECS, PTX_RECS, IPS_RECS, EPG_REC,
		IPO_RECS;
	}

	@Override
	public DerivedData validate(Messenger messenger, RawData rawDataIn) throws StatementSchemaException,
			StatementFormatException, StatementCustomException, StatementNonFatalException, IOException {
		long startTime = System.nanoTime();
		multiFmtr = MultiFmtr.getInstance();
		MyRawData rawData = (MyRawData) rawDataIn;
		Statement derivedRawStmt = null;

		String infactAccountNum = null;
		try {
			infactAccountNum = rawData.statement.getMetaData().getString("infact_account_number");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (isBlank(infactAccountNum)) {
			messenger.setErrMsgAccount("UKNOWN");
			throw new StatementCustomException("Unable to find account number.");
		} else {
			messenger.setErrMsgAccount(infactAccountNum);
		}
		try {
			derivedRawStmt = modifyAFP(rawData);
		} catch (IOException | StatementNonFatalException | StatementCustomException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		PerformanceMap.addDuration("validate", System.nanoTime() - startTime);

		return new MyDerivedData(derivedRawStmt, rawData);
	}

	private Statement modifyAFP(MyRawData rawData)
			throws IOException, StatementNonFatalException, StatementCustomException, JSONException {
		long startTime = System.nanoTime();
		Statement stmt = rawData.statement;
		boolean doScrapeMoveAddress = false;

		// extract address from position
		List<FileCoordinate> fileCoordinatesList = new ArrayList<>();
		DataPrepConfig config = DataPrepConfig.get();
		List<HashMap<String, String>> addressList = new ArrayList<>();
		msger.logDebug("MAIL PIECE IN RAW DATA:" + rawData.statement.getMetaData());
		List<List<AfpRec>> docAddressAfpRecList = new ArrayList<>();
		List<AfpRec> docAddress = null;
		String foreignSHCode = "78";

		Object isbulkShippingMetaDataValue = rawData.statement.getMetaData().get("is_bulk_shipping");
		if (isbulkShippingMetaDataValue != null) {
			String isbulkShipping = isbulkShippingMetaDataValue.toString().trim();
			validateBulkShippingFlag(isbulkShipping);
		}
		Map<String, Map<String, Object>> cdfsAccountNumberData = config.getCREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS();
		List<Map<String, Object>> shippingSHCodeLookupConfig = config.getSHIPPING_SH_CODE_LOOKUP();
		List<Map<String, String>> tle = config.getINSERT_MAIL_PIECE_TLES();
		List<Map<String, String>> nop = config.getINSERT_MAIL_PIECE_NAMED_NOPS();

		// start processing send address
		Map<String, Object> sendAddressMap = config.getSCRAPE_SEND_ADDRESS();
		Map<String, Object> scrapeAndMoveAddressMap = config.getSCRAPE_AND_MOVE_SEND_ADDRESS();
		Map<String, Object> finalSendAddressMap = new HashMap<>();
		HashMap<String, Object> fromAddressMap = new HashMap<>();
		HashMap<String, Object> toAddressMap = new HashMap<>();
		if (scrapeAndMoveAddressMap != null) {
			Object fromAddress = scrapeAndMoveAddressMap.get("from");
			Object toAddress = scrapeAndMoveAddressMap.get("to");
			fromAddressMap = new Gson().fromJson(fromAddress.toString(), HashMap.class);
			toAddressMap = new Gson().fromJson(toAddress.toString().trim(), HashMap.class);
		}
		String isBulkShipping = rawData.statement.getMetaData().getString("is_bulk_shipping");
		if (isBulkShipping.equalsIgnoreCase("Y")) {
			addressLinesMap = populateAddressLines(rawData.statement.getMetaData());
			foreignSHCode = "93";
			if ((scrapeAndMoveAddressMap == null) && (sendAddressMap == null)) {
				List<String> addressLines = new ArrayList<>(addressLinesMap.values());
				docAddress = setAddressTLE(rawData, addressLines, addressList, docAddressAfpRecList, foreignSHCode,
						false);
			}
		} else {
			if ((sendAddressMap == null || sendAddressMap.isEmpty())
					&& (scrapeAndMoveAddressMap == null || scrapeAndMoveAddressMap.isEmpty())) {
				List<String> tleAddressLines = CommonUtils
						.extractAddressLinesFromInputTLEs(rawData.statement.getStatementRecords());
				if (!tleAddressLines.isEmpty()) {
					docAddress = setAddressTLE(rawData, tleAddressLines, addressList, docAddressAfpRecList, foreignSHCode,
							true);
				}
			}
		}
		if (null != sendAddressMap) {
			finalSendAddressMap = sendAddressMap;
		} else if (null != fromAddressMap) {
			finalSendAddressMap = fromAddressMap;
		}
		if (null != finalSendAddressMap && !finalSendAddressMap.isEmpty()) {
			JSONArray sendAddressArray = new JSONArray();
			double sx1 = 0.0;
			double sy1 = 0.0;
			double sx2 = 0.0;
			double sy2 = 0.0;
			int iOrientations = 0;
			docAddress = extractAddressPopulateTLE(rawData, fileCoordinatesList, finalSendAddressMap, sendAddressArray,
					addressList, docAddressAfpRecList, sx1, sy1, sx2, sy2, iOrientations, foreignSHCode,
					doScrapeMoveAddress);
			for (Map<String, String> map : addressList) {
				msger.logDebug("SCRAPED_ADDRESS");
				for (Map.Entry<String, String> entry : map.entrySet()) {
					msger.logDebug("  " + entry.getKey() + ": " + entry.getValue());
				}
			}

			List<AfpRec> statementRecords = rawData.statement.getStatementRecords();
			List<AfpRec> statementHeader = extractStatementHeader(rawData.statement);
			int insertPos = statementRecords.indexOf(statementHeader.get(statementHeader.size() - 1)) + 1;
			statementRecords.addAll(insertPos, docAddress);

			// Move address
			List<FileCoordinate> movefileCoordinatesList = new ArrayList<>();
			sendAddressArray = new JSONArray();
			getFileCoordinates(movefileCoordinatesList, fromAddressMap, sendAddressArray, sx1, sy1, sx2, sy2,
					iOrientations);
			getFileCoordinates(movefileCoordinatesList, toAddressMap, sendAddressArray, sx1, sy1, sx2, sy2,
					iOrientations);
			if (addressList.size() > 0 && !toAddressMap.isEmpty()) {
				doScrapeMoveAddress = true;

				msger.logDebug("movefileCoordinatesList:" + movefileCoordinatesList.size());
				List<AfpRec> firstPageAfpRecs = rawData.statement.getFirstPage();
				AfpRec recMCF = textSpecification.addMCFToPage(firstPageAfpRecs, toAddressMap);
				if (null != recMCF) {
					ListIterator<AfpRec> it = firstPageAfpRecs.listIterator();
					while (it.hasNext()) {
						AfpRec r = it.next();
						if (r.getTla().equalsIgnoreCase("mcf1"))
							it.add(recMCF);
					}
				}
				firstPageAfpRecs = rawData.statement.getFirstPage();
				HashMap<String, String> lineSpaces = textSpecification.extractMoveAddressLines(firstPageAfpRecs,
						movefileCoordinatesList, doScrapeMoveAddress, toAddressMap);
				extractedRemoveMethod(rawData, movefileCoordinatesList);
				List<AfpRec> ptxRecs = textSpecification.createAddressRec(firstPageAfpRecs, movefileCoordinatesList,
						toAddressMap, lineSpaces);
				if (isNotEmpty(ptxRecs)) {
					ListIterator<AfpRec> it = firstPageAfpRecs.listIterator();
					while (it.hasNext()) {
						AfpRec r = it.next();
						if (r.getTla().equalsIgnoreCase("bpt")) {
							for (AfpRec ptxRec : ptxRecs)
								it.add(ptxRec);
						}
					}
				}
				msger.logDebug("recPTX:" + ptxRecs.size());

			}
		}
		// End of send address processing

		// Load config Hopper Lookup
		Map<String, Integer> hopperLookupConfig = config.getINSERT_ID_HOPPER_LOOKUP();
		if (hopperLookupConfig == null || hopperLookupConfig.isEmpty()) {
			msger.logDebug("No hopper lookup configuration found in dataprep config.");
			hopperLookupConfig = null;
		}

		JSONObject mailPiece = rawData.statement.getMetaData();
		Set<String> insertedTles = new HashSet<>();
		Set<String> insertedNops = new HashSet<>();
		List<AfpRec> hdrList = extractStatementHeader(stmt);
		for (AfpRec hdr : hdrList) {
			if (hdr.getTla().equals("BNG")) {
				if (mailPiece != null) {

					List<AfpRec> newTleRecords = new ArrayList<>(extractStatementHeader(stmt));
					List<AfpRec> newHeaderStatementRecordList = insertMailPieceLevelTles(extractStatementHeader(stmt),
							newTleRecords, mailPiece, insertedTles, tle, addressList, shippingSHCodeLookupConfig,
							config, rawData);
					List<AfpRec> updatedHeaderStatementList = new ArrayList<>();
					updatedHeaderStatementList.addAll(newHeaderStatementRecordList);
					boolean pastHeader = false;
					for (AfpRec rec : stmt.getStatementRecords()) {
						if (pastHeader || "BPG".equals(rec.getTla())) {
							pastHeader = true;
							updatedHeaderStatementList.add(rec);
						}
					}
					stmt.setStatementRecords(updatedHeaderStatementList);
				}

				if (docAccountNumberAsASeprateTle && config.getCREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS() != null
						&& config.getCREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS().size() > 0) {
					List<AfpRec> statementHeader = new ArrayList<>(extractStatementHeader(stmt));
					AfpRec accountNumberTle = new AfpCmdTLE(ACCOUNT_NUMBER,
							validateCDFS(cdfsAccountNumberData, rawData)).toAfpRec((short) 0, 0);
					msger.logDebug(accountNumberTle.toString());
					statementHeader.add(accountNumberTle);
					List<AfpRec> updatedStatementRecords = new ArrayList<>();
					updatedStatementRecords.addAll(statementHeader);
					boolean headerCompleted = false;
					for (AfpRec record : stmt.getStatementRecords()) {
						if (headerCompleted || "BPG".equals(record.getTla())) {
							headerCompleted = true;
							updatedStatementRecords.add(record);
						}
					}
					stmt.setStatementRecords(updatedStatementRecords);
				}

				if (mailPiece != null) {
					List<AfpRec> newNopRecords = new ArrayList<>(extractStatementHeader(stmt));
					newNopRecords = insertMailPieceLevelNamedNops(extractStatementHeader(stmt), newNopRecords,
							mailPiece, insertedNops, nop, addressList);

					List<AfpRec> updatedHeaderStatementList = new ArrayList<>();
					updatedHeaderStatementList.addAll(newNopRecords);
					boolean pastHeader = false;
					for (AfpRec rec : stmt.getStatementRecords()) {
						if (pastHeader || "BPG".equals(rec.getTla())) {
							pastHeader = true;
							updatedHeaderStatementList.add(rec);
						}
					}
					stmt.setStatementRecords(updatedHeaderStatementList);

				}
			}
		}

		if (hopperLookupConfig != null && mailPiece != null) {
			List<AfpRec> comboTleRecords = insertDocInsertComboTle(mailPiece, stmt, //
					Collections.singletonList(hopperLookupConfig), insertedTles);
			stmt.setStatementRecords(comboTleRecords); // update the statement records
		}

		// IMM Translate
		List<Map<String, String>> translateImmRecsConfig = config.getTRANSLATE_IMM_RECS();
		if (translateImmRecsConfig != null && !translateImmRecsConfig.isEmpty()) {
			translateImmRecs(stmt, translateImmRecsConfig);
		} else {
			msger.logDebug("TRANSLATE_IMM_RECS config is null or empty");
		}

		// ADD IMM Records
		int bpgCounter = 0;
		Map<String, AfpRec> imms = setImmRecs(stmt.getStatementRecords(), immSettings, immRecords, config);
		msger.logDebug("IMMS map size: " + (imms != null ? imms.size() : 0));
		List<AfpRec> updatedImmRecords = new ArrayList<>();
		for (AfpRec rec : stmt.getStatementRecords()) {
			if (rec != null && "BPG".equals(rec.getTla())) {
				bpgCounter++;
				if (imms != null && imms.containsKey(String.valueOf(bpgCounter))) {
					AfpRec immRecord = imms.get(String.valueOf(bpgCounter));

					if (immRecord != null) {
						updatedImmRecords.add(immRecord);
					} else {
						msger.printMsg("IMM record for BPG " + bpgCounter + " is null.");
					}
				}
			}
			updatedImmRecords.add(rec);
		}
		stmt.setStatementRecords(updatedImmRecords);

		// ADD IPS
		Map<String, AfpRec> pSegs = insertPageSegs(stmt.getStatementRecords(), pSegsSettings, pSegsRecords, config);
		List<AfpRec> updatedRecords = processPageSegments(stmt.getStatementRecords(), pSegs);
		stmt.setStatementRecords(updatedRecords);

		// EACH_PDF_DOCUMENT_STARTS_NEW_SHEET
		String eachPdfNewSheet = config.getEACH_PDF_DOCUMENT_STARTS_NEW_SHEET();
		if (null != eachPdfNewSheet && eachPdfNewSheet.equalsIgnoreCase("Y")) {
			HashMap<Integer, Integer> newSheets = getNewSheetPages(rawData);
			Map<String, AfpRec> immAfpMap = addIMMForDocument(stmt.getStatementRecords(), newSheets);
			List<AfpRec> modifiedStmtPages = setIMMRecToPage(stmt.getStatementRecords(), immAfpMap);
			stmt.setStatementRecords(modifiedStmtPages);
		}

		// Delete Text
		fileCoordinates.clear();
		List<Map<String, Object>> deleteTextConfig = config.getDELETE_TEXT();
		for (Map<String, Object> deleteEntry : deleteTextConfig) {
			int pagenum = (Integer) deleteEntry.get(PAGE_NUM);
			double x1 = (Double) deleteEntry.get("x1");
			double y1 = (Double) deleteEntry.get("y1");
			double x2 = (Double) deleteEntry.get("x2");
			double y2 = (Double) deleteEntry.get("y2");
			int iOrientation = (Integer) deleteEntry.get("orientation");
			fileCoordinates.add(new FileCoordinate(x1, y1, x2, y2, iOrientation, pagenum));
		}
		extractedRemoveMethod(rawData, fileCoordinates);

		// ADD Text
		List<Map<String, Object>> addTextToPagesConfig = config.getADD_TEXT_TO_PAGES();
		if (addTextToPagesConfig != null && !addTextToPagesConfig.isEmpty()) {
			textSpecification.insertTextBlocksIntoPages(stmt, addTextToPagesConfig);
		}

		insertIPOAfterEAG(stmt);

		insertNewPages(stmt);

		PerformanceMap.addDuration("modifyAFP", System.nanoTime() - startTime);

		return stmt;
	}

	public Map<String, AfpRec> insertPageSegs(List<AfpRec> afpRecList, Map<String, String> pSegsSettings,
			List<AfpRec> pSegsRecords, DataPrepConfig config)
			throws StatementCustomException, StatementNonFatalException {

		List<Map<String, Object>> insertPsegsArray = config.getINSERT_PSEGS();

		if (insertPsegsArray == null || insertPsegsArray.isEmpty()) {
			msger.logDebug("NO_PAGE_SEGMENT_FOUND in dataprep config");
			return null;
		}

		if (pSegsSettings == null) {
			pSegsSettings = new HashMap<>();
		}

		// Load PSEG settings from config
		for (Map<String, Object> entry : insertPsegsArray) {
			String pageNum = String.valueOf(entry.get(PAGE_NUM));
			String psegName = String.valueOf(entry.get("pseg_name"));
			String xOffset = String.valueOf(entry.get("x_offset"));
			String yOffset = String.valueOf(entry.get("y_offset"));

			if (psegName != null && !psegName.isEmpty()) {
				pSegsSettings.put(pageNum, psegName);
				pSegsSettings.put(pageNum + "_x_offset", xOffset);
				pSegsSettings.put(pageNum + "_y_offset", yOffset);
			}
		}

		if (afpRecList == null || afpRecList.isEmpty()) {
			msger.logDebug("No records to process.");
			return null;
		}

		if (pSegsRecords == null) {
			pSegsRecords = new ArrayList<>();
		}

		Set<String> insertedPsegPages = new HashSet<>();
		Set<String> availablePages = new HashSet<>();
		int pageNumber = 0;

		for (AfpRec afpRec : afpRecList) {
			if (afpRec == null)
				continue;

			if ("EAG".equals(afpRec.getTla())) {
				pageNumber++;
				String pageNumberStr = String.valueOf(pageNumber);
				availablePages.add(pageNumberStr);

				if (pSegsSettings.containsKey(pageNumberStr)) {
					String pSegToUse = pSegsSettings.get(pageNumberStr);
					String xOffset = pSegsSettings.get(pageNumberStr + "_x_offset");
					String yOffset = pSegsSettings.get(pageNumberStr + "_y_offset");

					if (pSegToUse != null && !pSegToUse.isEmpty()) {
						double pageDPI = CommonUtils.getPageDPIFromPGD(afpRecList, pageNumber);
						msger.logDebug("Inserting PSEG at Page: " + pageNumber);
						afpPsegMap = setPSegForPage(afpRec, pSegToUse, pageNumberStr, pSegsRecords, xOffset, yOffset,
								pageDPI);
						insertedPsegPages.add(pageNumberStr);
					}
				}
			}
		}

		// Check for missing EAGs
		Set<String> missingEagPages = new HashSet<>();
		for (String key : pSegsSettings.keySet()) {
			if (!key.contains("_") && availablePages.contains(key) && !insertedPsegPages.contains(key)) {
				missingEagPages.add(key);
			}
		}

		if (!missingEagPages.isEmpty()) {
			String errorMessage = " Missing EAG for pages: " + String.join(", ", missingEagPages);
			msger.logDebug(errorMessage);
			throw new StatementCustomException(errorMessage);
		}

		return afpPsegMap;
	}

	private Map<String, AfpRec> setPSegForPage(AfpRec afpRec, String pSegName, String pageNumberStr,
			List<AfpRec> pSegRecords, String xOffset, String yOffset, double DPI)
			throws StatementNonFatalException, StatementCustomException {

		if (afpRec != null && pSegRecords != null) {
			SBIN3 aXpsOset = new SBIN3(inchToDP(xOffset, Double.toString(DPI)));
			SBIN3 aYpsOset = new SBIN3(inchToDP(yOffset, Double.toString(DPI)));
			AfpRec updatedRec = addCmdIPS(pSegName, aXpsOset, aYpsOset);
			if (updatedRec != null && !pSegRecords.contains(updatedRec)) {
				pSegRecords.add(updatedRec);
				if (afpPsegMap != null)
					afpPsegMap.put(pageNumberStr, updatedRec);
			}
		}

		return afpPsegMap;
	}

	private List<AfpRec> processPageSegments(List<AfpRec> records, Map<String, AfpRec> pSegs) {
		List<AfpRec> updatedRecords = new ArrayList<>();
		int pageCounter = 0;

		for (AfpRec rec : records) {
			updatedRecords.add(rec);
			if (rec != null && "EAG".equals(rec.getTla())) {
				pageCounter++;
				if (pSegs != null && pSegs.containsKey(String.valueOf(pageCounter))) {
					AfpRec psegRec = pSegs.get(String.valueOf(pageCounter));
					if (psegRec != null) {
						updatedRecords.add(psegRec);
					}
				}
			}
		}

		return updatedRecords;
	}

	private void validateBulkShippingFlag(String isbulkShipping) throws StatementCustomException {
		String envBulkShipping = System.getenv("IS_BULK_SHIPPING");
		if (isbulkShipping != null && null != envBulkShipping) {
			if (!envBulkShipping.equalsIgnoreCase(isbulkShipping)) {
				String errorMessage = String.format(
						"Mismatch in bulk shipping status: Environment(IS_BULK_SHIPPING=%s) vs Metadata(is_bulk_shipping=%s)",
						envBulkShipping, isbulkShipping);
				throw new StatementCustomException(errorMessage);
			}
		} else {
			msger.logDebug(" isbulkShipping or envBulkShipping found null ");
		}
	}

	private static Map<String, String> populateAddressLines(JSONObject mailpiece) throws StatementCustomException {
		Map<String, String> addressMap = new LinkedHashMap<>();

		JSONArray addressLines = mailpiece.optJSONArray("send_address_lines");

		if (null == mailpiece.optJSONArray("send_address_lines")) {
			throw new StatementCustomException("send_address_lines is coming null from meatadata");
		}
		if (addressLines == null || addressLines.length() == 0) {
			throw new StatementCustomException("send_address_lines is coming empty from meatadata");

		}
		if (addressLines != null && addressLines.length() > 6) {
			throw new StatementCustomException("send_address_lines cannot have more than 6 lines");
		}
		for (int i = 0; i < 6; i++) {
			String key = "addressline" + (i + 1);
			String value = "";
			if (addressLines != null && i < addressLines.length()) {
				value = addressLines.optString(i, "").trim();
			}

			addressMap.put(key, value);
		}

		return addressMap;
	}

	public String validateCDFS(Map<String, Map<String, Object>> configObj, MyRawData rawData)
			throws StatementCustomException, StatementNonFatalException {
		Map<String, Map<String, Object>> updatedConfig = new HashMap<>();

		for (Map.Entry<String, Map<String, Object>> entry : configObj.entrySet()) {
			String fieldKey = entry.getKey();
			Map<String, Object> fieldConfig = entry.getValue();

			if (fieldConfig == null) {
				updatedConfig.put(fieldKey, null);
				continue;
			}
			List<String> requiredKeys = List.of("value", "size", "justification", "padding_char");
			for (String key : requiredKeys) {
				if (!fieldConfig.containsKey(key)) {
					msger.printMsg("Missing required field '" + key + "' in CREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS -> "
							+ fieldKey);
				}
			}
			String rawValue = (String) fieldConfig.get("value");
			Number sizeNumber = (Number) fieldConfig.get("size");
			int size = sizeNumber.intValue();
			String justification = (String) fieldConfig.get("justification");
			String paddingCharStr = (String) fieldConfig.get("padding_char");
			char paddingChar = (paddingCharStr != null && !paddingCharStr.isEmpty()) ? paddingCharStr.charAt(0) : ' ';
			String actualValue;
			if (rawValue.contains("FROM_METADATA^")) {
				String metaKey = rawValue.replace("FROM_METADATA^", "").replace("^", "");
				actualValue = rawData.statement.getMetaData().getString(metaKey);
				if (actualValue == null || actualValue.trim().length() == 0)
					actualValue = "";

			} else
				actualValue = rawValue;

			// Apply justification and padding
			String formattedValue = justifyAndPad(actualValue, size, justification, paddingChar);

			// Build new config map entry
			Map<String, Object> updatedFieldConfig = new HashMap<>(fieldConfig);
			updatedFieldConfig.put("value", formattedValue);
			updatedConfig.put(fieldKey, updatedFieldConfig);
		}

		StringBuilder result = new StringBuilder();
		int totalLength = 0;

		// Process account_num first
		totalLength += appendFormattedField(result, updatedConfig.get("account_num"), "account_num");

		// Process up to 4 CDF fields
		for (int i = 1; i <= 4; i++) {
			String key = "cdf_" + i;
			if (updatedConfig.containsKey(key)) {
				Map<String, Object> fieldConfig = updatedConfig.get(key);
				if (fieldConfig != null) {
					totalLength += appendFormattedField(result, fieldConfig, key);
				}
			}
		}

		if (totalLength > 50) {
			throw new StatementCustomException("Combined field length exceeds 50 characters.");
		}

		return result.toString();
	}

	private String justifyAndPad(String value, int size, String justification, char paddingChar) {
		if (value == null)
			value = "";
		int paddingLength = size - value.length();
		if (value.trim().length() <= size) {
			String padding = String.valueOf(paddingChar).repeat(paddingLength);
			justification = justification.toLowerCase();
			if ("left".equals(justification)) {
				return value + padding;
			} else if ("right".equals(justification)) {
				return padding + value;
			} else {
				throw new IllegalArgumentException("Invalid justification: " + justification);
			}
		} else {
			throw new RuntimeException("Value in field '" + value + "' doesn't match its size limit.");
		}
	}

	private int appendFormattedField(StringBuilder result, Map<String, Object> field, String fieldName)
			throws StatementNonFatalException, StatementCustomException {
		String paddedValue = null;
		try {

			if (field != null) {
				String value = (String) field.get("value");
				Number sizeNumber = (Number) field.get("size");
				Integer size = sizeNumber.intValue();

				String justification = (String) field.get("justification");
				String paddingChar = (String) field.get("padding_char");

				// Validate required attributes
				if (value == null || size == null || justification == null || paddingChar == null) {
					throw new RuntimeException("Missing attributes in field: " + fieldName);
				}

				if (!(value.length() == size)) {
					throw new RuntimeException(
							"Value in field '" + fieldName + "' doesn't match its size limit after padding");
				}

				if (!justification.equalsIgnoreCase("left") && !justification.equalsIgnoreCase("right")) {
					throw new RuntimeException("Invalid justification in field: " + fieldName);
				}

				paddedValue = justify(value, size, justification, paddingChar);
				result.append(paddedValue);
			}
			return paddedValue.length();
		} catch (NullPointerException e) {
			e.printStackTrace();
			throw new StatementCustomException(e.getMessage());
		}

	}

	private String justify(String value, int size, String justification, String padChar) {
		StringBuilder sb = new StringBuilder();
		int padding = size - value.length();

		if (justification.equalsIgnoreCase("left")) {
			sb.append(value);
			sb.append(padChar.repeat(padding));
		} else { // right
			sb.append(padChar.repeat(padding));
			sb.append(value);
		}

		return sb.toString();
	}

	public MyAnalyzer createNewInstance() throws IOException {
		return new MyAnalyzer();

	}

	public Map<String, AfpRec> setImmRecs(List<AfpRec> statementRecords, Map<String, String> immSettings,
			List<AfpRec> immRecords, DataPrepConfig config) throws StatementCustomException {

		if (immSettings == null) {
			immSettings = new HashMap<>();
		}

		List<Map<String, String>> setImmList = config.getSET_IMM_RECS();
		for (Map<String, String> immMap : setImmList) {
			immSettings.putAll(immMap);
		}

		if (statementRecords == null || statementRecords.isEmpty()) {
			return null;
		}

		if (immRecords == null) {
			immRecords = new ArrayList<>();
		}

		int pageNumber = 0;
		for (AfpRec rec : statementRecords) {
			if (rec == null)
				continue;

			if ("BPG".equals(rec.getTla())) {
				pageNumber++;
				String pageStr = String.valueOf(pageNumber);

				if (immSettings.containsKey(pageStr)) {
					String immValue = immSettings.get(pageStr);
					if (immValue != null && !immValue.isEmpty()) {
						if (immValue.length() > 8) {
							throw new StatementCustomException(
									"IMM value '" + immValue + "' is longer than 8 characters (Page: " + pageStr + ")");
						} else if (immValue.length() < 8) {
							immValue = String.format("%-8s", immValue);
						}

						afpMap = setImmForPage(rec, immValue, pageStr, immRecords);
					}
				}
			}
		}

		return afpMap;
	}

	private Map<String, AfpRec> setImmForPage(AfpRec afpRec, String immName, String pageNumberStr,
			List<AfpRec> immRecords) throws StatementCustomException {

		if (afpRec != null && immRecords != null) {
			AfpCmdIMM afpCmdImm = new AfpCmdIMM(String.format("%-8s", immName));
			AfpRec updatedRec;
			try {
				updatedRec = afpCmdImm.toAfpRec((short) 0, 0);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				throw new StatementCustomException(e.getMessage());
			}
			if (updatedRec != null) {
				immRecords.add(updatedRec);
				if (afpMap != null) {
					afpMap.put(pageNumberStr, updatedRec);
				}
			}
		}

		return afpMap;
	}

	public List<AfpRec> setSendAddressTles(HashMap<String, String> docAddressRecords) throws StatementCustomException {
		List<AfpRec> addressTleRecords = new ArrayList<>();

		Gson gson = new Gson();
		String jsonString = gson.toJson(docAddressRecords);
		msger.logDebug("jsonString:" + jsonString);
		JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
		Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();

		for (Map.Entry<String, JsonElement> entry : entrySet) {
			String key = entry.getKey();
			JsonElement value = entry.getValue();

			String tleValue = value.getAsString();
			if (tleValue != null) {
				AfpRec newRec;
				try {
					newRec = (new AfpCmdTLE(key, tleValue)).toAfpRec((short) 0, 0);
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
					throw new StatementCustomException(e.getMessage());
				}
				addressTleRecords.add(newRec);

			}

		}

		return addressTleRecords;
	}

	private List<AfpRec> extractAddressPopulateTLE(MyRawData rawData, List<FileCoordinate> fileCoordinatesList,
			Map<String, Object> sendAddressMap, JSONArray sendAddressArray, List<HashMap<String, String>> addressList,
			List<List<AfpRec>> docAddressAfpRecList, double sx1, double sy1, double sx2, double sy2, int iOrientations,
			String foreignSHCode, boolean doScrapeMoveAddress)
			throws StatementNonFatalException, StatementCustomException, IOException {
		List<AfpRec> docAddress;
		getFileCoordinates(fileCoordinatesList, sendAddressMap, sendAddressArray, sx1, sy1, sx2, sy2, iOrientations);
		List<AfpRec> firstPageAfpRecs = rawData.statement.getFirstPage();
		List<String> scrapedAdressLines = textSpecification.extractTextAt(firstPageAfpRecs, fileCoordinatesList,
				doScrapeMoveAddress, sendAddressMap);
		msger.logDebug("Address:" + Arrays.asList(scrapedAdressLines));
		if (scrapedAdressLines.isEmpty()) {
			docAccountNumber = rawData.statement.getMetaData().getString("infact_account_number");
			msger.printMsg("NO_SEND_ADDRESS_FOUND", docAccountNumber);
			throw new StatementCustomException("No address lines scraped for" + docAccountNumber);
		}
		docAddress = setAddressTLE(rawData, scrapedAdressLines, addressList, docAddressAfpRecList, foreignSHCode,
				false);
		return docAddress;
	}

	private void getFileCoordinates(List<FileCoordinate> fileCoordinatesList, Map<String, Object> sendAddressMap,
			JSONArray sendAddressArray, double sx1, double sy1, double sx2, double sy2, int iOrientations)
			throws JSONException {

		for (Map.Entry<String, Object> entry : sendAddressMap.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			JSONObject jsonObject = new JSONObject();
			jsonObject.put("key", key);
			jsonObject.put("value", value);
			msger.logDebug("key:value " + key + ":" + value);

			if ("x1".equals(key)) {
				sx1 = Double.parseDouble(value.toString());
			} else if ("y1".equals(key)) {
				sy1 = Double.parseDouble(value.toString());
			} else if ("x2".equals(key)) {
				sx2 = Double.parseDouble(value.toString());
			} else if ("y2".equals(key)) {
				sy2 = Double.parseDouble(value.toString());
			} else if ("orientation".equals(key)) {
				iOrientations = (int) Double.parseDouble(value.toString());
			}

			sendAddressArray.put(jsonObject);
		}

		List<Integer> validOrientations = Arrays.asList(0, 90, 180, 270);
		if (!validOrientations.contains(iOrientations)) {
			msger.printMsg("FATAL", "Invalid orientation value for address scraping: " + iOrientations
					+ " degrees; valid values are: 0, 90, 180, 270");
		}

		fileCoordinatesList.add(new FileCoordinate(sx1, sy1, sx2, sy2, iOrientations, 1));
	}

	private List<AfpRec> setAddressTLE(MyRawData rawData, List<String> addressLines,
			List<HashMap<String, String>> addressList, List<List<AfpRec>> docAddressAfpRecList, String foreignSHCode,
			boolean skipTLECreation) throws JSONException, StatementNonFatalException, StatementCustomException {

		List<AfpRec> docAddress = new ArrayList<>();
		HashMap<String, String> addressMap;

		addressMap = populateAddressMap(addressLines, rawData.statement.getMetaData(), foreignSHCode);
		addressList.add(addressMap);

		if (!skipTLECreation) {
			docAddress = setSendAddressTles(addressMap);
			docAddressAfpRecList.add(docAddress);
		} else {
			String zipCode = addressMap.get("DOC_SEND_ADDRESS_ZIP_CODE");
			if (zipCode != null) {
				HashMap<String, String> zipCodeMap = new HashMap<String, String>();
				zipCodeMap.put("DOC_SEND_ADDR_ZIP_CODE", zipCode);

				docAddress = setSendAddressTles(zipCodeMap);
			}
		}

		return docAddress;
	}

	/*
	 * private Map<String, AfpRec> setPSegForPage(AfpRec afpRec, String pSegName,
	 * String pageNumberStr, List<AfpRec> pSegRecords, String xOffset, String
	 * yOffset, double DPI) throws StatementNonFatalException,
	 * StatementCustomException {
	 * 
	 * if (afpRec != null && pSegRecords != null) { SBIN3 aXpsOset = new
	 * SBIN3(inchToDP(xOffset, Double.toString(DPI))); SBIN3 aYpsOset = new
	 * SBIN3(inchToDP(yOffset, Double.toString(DPI))); AfpRec updatedRec =
	 * addCmdIPS(pSegName, aXpsOset, aYpsOset); if (updatedRec != null) { if
	 * (!pSegRecords.contains(updatedRec)) { pSegRecords.add(updatedRec); if
	 * (afpPsegMap != null) { afpPsegMap.put(pageNumberStr, updatedRec); } } } }
	 * 
	 * return afpPsegMap; }
	 */

	public List<AfpRec> insertMailPieceLevelTles(List<AfpRec> afpRecList, List<AfpRec> tleRecords, JSONObject mailPiece,
			Set<String> insertedTles, List<Map<String, String>> tle, List<HashMap<String, String>> addressList,
			List<Map<String, Object>> shippingSHCodeLookupConfig, DataPrepConfig config, MyRawData rawData)
			throws StatementNonFatalException, StatementCustomException, JSONException {

		if (afpRecList == null || tleRecords == null || insertedTles == null || mailPiece == null || tle == null) {
			return tleRecords;
		}

		insertedTles.clear();
		List<Map<String, String>> tleConfig = getDynamicConfig(mailPiece, tle, "FROM_METADATA:");
		List<Map<String, String>> updatedtleConfigList = new ArrayList<>();

		if (config.getINSERT_MAIL_PIECE_TLES() != null && !config.getINSERT_MAIL_PIECE_TLES().isEmpty()
				&& config.getCREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS() != null
				&& !config.getCREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS().isEmpty()) {

			String docAccountNumberWithCDFSData = null;
			Map<String, Map<String, Object>> cdfsAccountNumberData = config.getCREATE_DOC_ACCOUNT_NUMBER_WITH_CDFS();
			if (!cdfsAccountNumberData.isEmpty()) {
				docAccountNumberWithCDFSData = validateCDFS(cdfsAccountNumberData, rawData);
			}

			for (Map<String, String> map : tleConfig) {
				if (map.containsKey(ACCOUNT_NUMBER) && docAccountNumberWithCDFSData != null) {
					Map<String, String> m = new HashMap<>();
					m.put(ACCOUNT_NUMBER, docAccountNumberWithCDFSData);
					updatedtleConfigList.add(m);
					docAccountNumberAsASeprateTle = false;
				} else {
					updatedtleConfigList.add(map);
				}
			}
			tleConfig = updatedtleConfigList;
		}

		List<AfpRec> current = new ArrayList<>();

		for (Map<String, String> tleEntry : tleConfig) {
			for (Map.Entry<String, String> e : tleEntry.entrySet()) {
				String key = e.getKey();
				Object raw = e.getValue();
				String v = (raw instanceof Number)
						? ((((Number) raw).doubleValue() == Math.floor(((Number) raw).doubleValue()))
								? String.valueOf(((Number) raw).longValue())
								: String.valueOf(raw))
						: String.valueOf(raw);

				Matcher d = Pattern.compile("FROM_DERIVEDDATA\\^([^\\^]+)\\^").matcher(v);
				StringBuffer sb1 = new StringBuffer();
				while (d.find()) {
					String r = (String) getDynamicCmdLineValue(d.group(1));
					if (r == null)
						throw new StatementCustomException(d.group(1) + "does not exist in CmdLine arguments");
					d.appendReplacement(sb1, Matcher.quoteReplacement(r));
				}
				d.appendTail(sb1);
				v = sb1.toString();

				Matcher m = Pattern.compile("FROM_METADATA\\^([^\\^]+)\\^").matcher(v);
				StringBuffer sb2 = new StringBuffer();
				while (m.find()) {
					Object meta = getValueFromMailPiece(mailPiece, m.group(1));
					String r;
					if (meta instanceof Number) {
						double n = ((Number) meta).doubleValue();
						r = (n == Math.floor(n)) ? String.valueOf((long) n) : String.valueOf(n);
					} else {
						r = String.valueOf(meta);
					}
					if (r == null)
						r = "";
					m.appendReplacement(sb2, Matcher.quoteReplacement(r));
				}
				m.appendTail(sb2);
				v = sb2.toString();

				String resolved = resolveDynamicPlaceholdersWithSendAddressOverride(v, mailPiece);
				String tleKey = key + ":" + resolved;

				boolean skip = false;
				for (HashMap<String, String> a : addressList) {
					if (SPECIAL_HANDLING_CODE.equals(key) && a.containsKey(SPECIAL_HANDLING_CODE)) {
						skip = true;
						break;
					}
				}

				if (!insertedTles.contains(tleKey) && !skip) {
					try {
						AfpRec rec = new AfpCmdTLE(key, resolved).toAfpRec((short) 0, 0);
						current.add(rec);
						insertedTles.add(tleKey);
					} catch (UnsupportedEncodingException ex) {
						throw new StatementCustomException("Error creating Afp TLE record");
					}
				}
			}
		}

		boolean overrideSH = false;
		String overrideSHCode = null;
		for (HashMap<String, String> a : addressList) {
			if (a.containsKey(SPECIAL_HANDLING_CODE)) {
				overrideSH = true;
				overrideSHCode = a.get(SPECIAL_HANDLING_CODE);
				break;
			}
		}

		if (overrideSH && overrideSHCode != null) {
			tleRecords.removeIf(rec -> "TLE".equals(rec.getTla()) && rec.toString().contains(SPECIAL_HANDLING_CODE));
			current.removeIf(rec -> "TLE".equals(rec.getTla()) && rec.toString().contains(SPECIAL_HANDLING_CODE));
			insertedTles.removeIf(k -> k.startsWith("DOC_SPECIAL_HANDLING_CODE:"));

			try {
				AfpRec rec = new AfpCmdTLE(SPECIAL_HANDLING_CODE, overrideSHCode).toAfpRec((short) 0, 0);
				current.add(rec);
				insertedTles.add("DOC_SPECIAL_HANDLING_CODE:" + overrideSHCode);
			} catch (UnsupportedEncodingException ex) {
				throw new StatementCustomException("Error creating Afp TLE record (override)");
			}
		} else {
			String shFromLookup = lookupSpecialHandlingCode(mailPiece, shippingSHCodeLookupConfig);
			if (shFromLookup != null) {
				tleRecords
						.removeIf(rec -> "TLE".equals(rec.getTla()) && rec.toString().contains(SPECIAL_HANDLING_CODE));
				current.removeIf(rec -> "TLE".equals(rec.getTla()) && rec.toString().contains(SPECIAL_HANDLING_CODE));
				insertedTles.removeIf(k -> k.startsWith("DOC_SPECIAL_HANDLING_CODE:"));

				String key = "DOC_SPECIAL_HANDLING_CODE:" + shFromLookup;
				if (!insertedTles.contains(key)) {
					try {
						AfpRec rec = new AfpCmdTLE(SPECIAL_HANDLING_CODE, shFromLookup).toAfpRec((short) 0, 0);
						current.add(rec);
						insertedTles.add(key);
					} catch (UnsupportedEncodingException ex) {
						throw new StatementCustomException("Error creating Afp TLE record (lookup)");
					}
				}
			}
		}

		tleRecords.addAll(current);
		return tleRecords;
	}

	public List<AfpRec> insertMailPieceLevelNamedNops(List<AfpRec> afpRecList, List<AfpRec> nopRecords,
			JSONObject mailPiece, Set<String> insertedNops, List<Map<String, String>> nop,
			List<HashMap<String, String>> addressList)
			throws StatementNonFatalException, StatementCustomException, JSONException {

		if (afpRecList == null || nopRecords == null || insertedNops == null || mailPiece == null || nop == null) {
			return nopRecords;
		}

		insertedNops.clear();
		List<Map<String, String>> nopConfig = getDynamicConfig(mailPiece, nop, "FROM_METADATA:");
		List<AfpRec> currentMailPieceNopRecords = new ArrayList<>();

		for (Map<String, String> nopEntry : nopConfig) {
			for (Map.Entry<String, String> entry : nopEntry.entrySet()) {
				String key = entry.getKey();
				Object rawValue = entry.getValue();

				String formattedValue;
				if (rawValue instanceof Number) {
					double val = ((Number) rawValue).doubleValue();
					formattedValue = (val == Math.floor(val)) ? String.valueOf((long) val) : String.valueOf(val);
				} else {
					formattedValue = String.valueOf(rawValue);
				}

				// Replace FROM_DERIVEDDATA^ and FROM_METADATA^
				Pattern derivedPattern = Pattern.compile("FROM_DERIVEDDATA\\^([^\\^]+)\\^");
				Matcher derivedMatcher = derivedPattern.matcher(formattedValue);
				StringBuffer derivedBuffer = new StringBuffer();
				while (derivedMatcher.find()) {
					String varName = derivedMatcher.group(1);
					String replacement = (String) getDynamicCmdLineValue(varName);
					if (replacement == null)
						throw new StatementCustomException(varName + "does not exist in CmdLine arguments");
					derivedMatcher.appendReplacement(derivedBuffer, Matcher.quoteReplacement(replacement));
				}
				derivedMatcher.appendTail(derivedBuffer);
				formattedValue = derivedBuffer.toString();

				Pattern metaPattern = Pattern.compile("FROM_METADATA\\^([^\\^]+)\\^");
				Matcher metaMatcher = metaPattern.matcher(formattedValue);
				StringBuffer metaBuffer = new StringBuffer();
				while (metaMatcher.find()) {
					String varName = metaMatcher.group(1);
					Object val = getValueFromMailPiece(mailPiece, varName);
					String resolved;
					if (val instanceof Number) {
						double d = ((Number) val).doubleValue();
						resolved = (d == Math.floor(d)) ? String.valueOf((long) d) : String.valueOf(d);
					} else {
						resolved = String.valueOf(val);
					}
					if (resolved == null)
						resolved = "";
					metaMatcher.appendReplacement(metaBuffer, Matcher.quoteReplacement(resolved));
				}
				metaMatcher.appendTail(metaBuffer);
				formattedValue = metaBuffer.toString();

				String resolvedValue = resolveDynamicPlaceholdersWithSendAddressOverride(formattedValue, mailPiece);
				String nopKey = key + ":" + resolvedValue;

				for (HashMap<String, String> scrapeAddressMap : addressList) {
					if (key.equals(SPECIAL_HANDLING_CODE) && scrapeAddressMap.containsKey(SPECIAL_HANDLING_CODE)) {
						for (Map.Entry<String, String> scrapeEntry : scrapeAddressMap.entrySet()) {
							String scrapeKey = scrapeEntry.getKey();
							String scrapeValue = scrapeEntry.getValue();

							if (scrapeKey.equals(SPECIAL_HANDLING_CODE)) {
								AfpRec newRec;
								try {
									newRec = new AfpNopGenericZD(scrapeKey, scrapeValue).toAfpRec((short) 0, 0);
								} catch (UnsupportedEncodingException e) {
									e.printStackTrace();
									throw new StatementCustomException(
											"In insertMailPieceLevelNamedNops:Error while creating Afp NOP rec");
								}
								currentMailPieceNopRecords.add(newRec);
								insertedNops.add(scrapeKey + ":" + scrapeValue);
							}
						}
					}
				}

				if (!insertedNops.contains(nopKey)) {
					AfpRec newRec;
					try {
						newRec = new AfpNopGenericZD(key, resolvedValue).toAfpRec((short) 0, 0);
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
						throw new StatementCustomException(
								"In insertMailPieceLevelNamedNops:Error while creating Afp NOP rec");
					}
					currentMailPieceNopRecords.add(newRec);
					insertedNops.add(nopKey);
				}
			}
		}

		nopRecords.addAll(currentMailPieceNopRecords);

		return nopRecords;
	}

	public List<AfpRec> insertFileHeaderNamedNops(List<AfpRec> headerRecords)
			throws IOException, StatementNonFatalException, StatementCustomException {

		boolean headerNopsInserted = false;
		if (headerNopsInserted) {
			return headerRecords;
		}

		try (JsonReader reader = MetadataReader.getMailPieceReader()) {
			while (reader.hasNext()) {
				JsonObject mailPiece = JsonParser.parseReader(reader).getAsJsonObject();
				JSONObject mailPieceJson = new JSONObject(mailPiece.toString());

				Set<String> insertedNops = new HashSet<String>();
				headerRecords = insertNamedNops(headerRecords, insertedNops,
						DataPrepConfig.get().getINSERT_FILE_HEADER_NAMED_NOPS(), mailPieceJson);
			}
			reader.endArray();
		} catch (JSONException e) {
			throw new IOException("Failed during NOP insertion", e);
		}

		headerNopsInserted = true;
		return headerRecords;
	}

	public List<AfpRec> insertNamedNops(List<AfpRec> afpRecList, Set<String> insertedNops,
			List<Map<String, String>> insertFileLevelNamedNopsL, JSONObject mailPiece)
			throws StatementNonFatalException, StatementCustomException, JSONException, IOException {

		if (insertFileLevelNamedNopsL == null || insertFileLevelNamedNopsL.isEmpty()) {
			return afpRecList;
		}

		JSONArray sendAddressLines = mailPiece.optJSONArray(SEND_ADDRESS_LINES);
		if (sendAddressLines == null) {
			sendAddressLines = new JSONArray();
		}

		for (Map<String, String> map : insertFileLevelNamedNopsL) {
			JSONObject item = convertMapToJson(map);
			if (item == null)
				continue;

			Iterator<String> keys = item.keys();
			while (keys.hasNext()) {
				String key = keys.next();
				Object value = item.opt(key);
				if (value == null)
					value = "";

				boolean addedNopBeforeBrg = false;

				for (int j = 0; j < afpRecList.size(); j++) {
					AfpRec afpRec1 = afpRecList.get(j);
					if (afpRec1 != null && "BRG".equalsIgnoreCase(afpRec1.getTla().trim())) {
						if (!addedNopBeforeBrg) {
							String nopValue = "";

							if (value instanceof String) {
								String valueStr = (String) value;
								nopValue = valueStr;

								Pattern derivedPattern = Pattern.compile("FROM_DERIVEDDATA\\^([^\\^]+)\\^");
								Matcher derivedMatcher = derivedPattern.matcher(nopValue);
								StringBuffer derivedBuffer = new StringBuffer();
								while (derivedMatcher.find()) {
									String varName = derivedMatcher.group(1);
									String replacement = (String) getDynamicCmdLineValue(varName);
									if (replacement == null) {
										throw new IOException(varName + " not found in derived data.");
									}
									derivedMatcher.appendReplacement(derivedBuffer,
											Matcher.quoteReplacement(replacement));
								}
								derivedMatcher.appendTail(derivedBuffer);
								nopValue = derivedBuffer.toString();

								nopValue = getAddressLineFromMetadata(sendAddressLines, nopValue);

								Pattern metaPattern = Pattern.compile("FROM_METADATA\\^([^\\^]+)\\^");
								Matcher metaMatcher = metaPattern.matcher(nopValue);
								StringBuffer metaBuffer = new StringBuffer();
								while (metaMatcher.find()) {
									String varName = metaMatcher.group(1);
									String replacement = getValueFromMailPiece(mailPiece, varName);
									if (replacement == null)
										replacement = "";
									metaMatcher.appendReplacement(metaBuffer, Matcher.quoteReplacement(replacement));
								}
								metaMatcher.appendTail(metaBuffer);
								nopValue = metaBuffer.toString();
							}

							String nopKey = key + ":" + nopValue;
							if (!insertedNops.contains(nopKey)) {
								AfpRec newRec;
								try {
									newRec = new AfpNopGenericZD(key, nopValue).toAfpRec((short) 0, 0);
								} catch (UnsupportedEncodingException e) {
									e.printStackTrace();
									throw new StatementCustomException("Error creating Afp NOP record");
								}
								afpRecList.add(j, newRec);
								insertedNops.add(nopKey);
							}

							addedNopBeforeBrg = true;
						}
					}
				}
			}
		}

		return afpRecList;
	}

	private String resolveDynamicPlaceholders(String input, JSONObject mailPiece)
			throws StatementNonFatalException, StatementCustomException, JSONException {
		if (input == null)
			return null;

		// Replace FROM_DERIVEDDATA^...^
		Pattern derivedPattern = Pattern.compile("FROM_DERIVEDDATA\\^([^\\^]+)\\^");
		Matcher derivedMatcher = derivedPattern.matcher(input);
		StringBuffer derivedBuffer = new StringBuffer();
		while (derivedMatcher.find()) {
			String varName = derivedMatcher.group(1);
			String value = (String) getDynamicCmdLineValue(varName);
			if (value == null) {
				msger.printMsg("Error: Derived data value for " + varName + " not found.");
				throw new StatementCustomException("Error: Derived data value for " + varName + " not found.");
			}
			derivedMatcher.appendReplacement(derivedBuffer, Matcher.quoteReplacement(value));
		}
		derivedMatcher.appendTail(derivedBuffer);

		// Replace FROM_METADATA^...^
		Pattern metaPattern = Pattern.compile("FROM_METADATA\\^([^\\^]+)\\^");
		Matcher metaMatcher = metaPattern.matcher(derivedBuffer.toString());
		StringBuffer finalBuffer = new StringBuffer();
		while (metaMatcher.find()) {
			String varName = metaMatcher.group(1);
			String value = getValueFromMailPiece(mailPiece, varName);
			if (value == null)
				value = ""; // treat null as empty string
			metaMatcher.appendReplacement(finalBuffer, Matcher.quoteReplacement(value));
		}
		metaMatcher.appendTail(finalBuffer);

		return finalBuffer.toString();
	}

	private List<Map<String, String>> getDynamicConfig(JSONObject mailPiece, List<Map<String, String>> configList,
			String prefix) throws StatementCustomException, JSONException {
		if (configList == null || mailPiece == null) {
			return new ArrayList<>();
		}

		List<Map<String, String>> dynamicConfig = new ArrayList<>();

		for (Map<String, String> config : configList) {
			Map<String, String> dynamicEntry = new HashMap<>();

			for (Map.Entry<String, String> entry : config.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();

				if (value != null && value.startsWith(prefix)) {
					String fieldName = value.substring(prefix.length());
					value = getValueFromMailPiece(mailPiece, fieldName);
				}

				dynamicEntry.put(key, value);
			}

			dynamicConfig.add(dynamicEntry);
		}

		return dynamicConfig;
	}

	public String getValueFromMailPiece(JSONObject mailPiece, String fieldName)
			throws StatementCustomException, JSONException {
		if (fieldName == null) {
			return "";
		}

		if (mailPiece == null) {
			throw new StatementCustomException("Error: mailPiece is null.");
		}

		if (fieldName.matches("send_address_line_[1-6]")) {
			int index = Integer.parseInt(fieldName.substring("send_address_line_".length())) - 1;
			if (mailPiece.has(SEND_ADDRESS_LINES)) {
				JSONArray addressLines = mailPiece.optJSONArray(SEND_ADDRESS_LINES);
				if (addressLines != null && index < addressLines.length()) {
					return addressLines.optString(index, "");
				}
			}
			return "";
		}

		if (mailPiece.has(fieldName)) {
			Object value = mailPiece.get(fieldName);
			return (value == null || value.equals(JSONObject.NULL)) ? "" : value.toString();
		}

		throw new StatementCustomException("Field '" + fieldName + "' not found in current mail piece.");
	}

	public List<AfpRec> insertFileTrailerNamedNops(List<AfpRec> trailerRecords)
			throws IOException, StatementNonFatalException, StatementCustomException {

		try {
			long totalMailPieces = MetadataReader.getTotalMailPieces();

			int totalPageCount = MetadataReader.getTotalPageCount();
			int sheetCount = (int) Math.ceil((double) totalPageCount / 2);
			if (sheetCount % 2 != 0) {
				sheetCount++;
			}
			JsonObject lastMailPiece = MetadataReader.getMailPieceByIndex((int) totalMailPieces - 1);
			Set<String> insertedNops = new HashSet<String>();
			trailerRecords = insertTrailerNops(trailerRecords, insertedNops,
					DataPrepConfig.get().getINSERT_FILE_TRAILER_NAMED_NOPS(), sheetCount, totalPageCount,
					new JSONObject(lastMailPiece.toString()), (int) totalMailPieces);

		} catch (JSONException e) {
			throw new IOException("Failed to parse trailer metadata", e);
		}

		return trailerRecords;
	}

	public List<AfpRec> insertTrailerNops(List<AfpRec> afpRecList, Set<String> insertedNops,
			List<Map<String, String>> insertTrailerNamedNopsL, int sheetCount, int pageCount, JSONObject mailPiece,
			int packages)
			throws StatementNonFatalException, StatementCustomException, UnsupportedEncodingException, JSONException {

		if (afpRecList == null || insertedNops == null || insertTrailerNamedNopsL == null
				|| insertTrailerNamedNopsL.isEmpty()) {
			return afpRecList;
		}

		JSONArray sendAddressLines = mailPiece.optJSONArray(SEND_ADDRESS_LINES);
		if (sendAddressLines == null) {
			sendAddressLines = new JSONArray();
		}
		while (sendAddressLines.length() < 6) {
			sendAddressLines.put("");
		}

		List<AfpRec> nopsToInsert = new ArrayList<>();

		for (Map<String, String> map : insertTrailerNamedNopsL) {
			JSONObject item = convertMapToJson(map);
			if (item == null)
				continue;

			Iterator<String> keys = item.keys();
			while (keys.hasNext()) {
				String key = keys.next();
				Object rawValue = item.opt(key);
				if (rawValue == null)
					rawValue = "";

				String metadataValue;
				if (rawValue instanceof Number) {
					double val = ((Number) rawValue).doubleValue();
					metadataValue = (val == Math.floor(val)) ? String.valueOf((long) val) : String.valueOf(val);
				} else {
					metadataValue = String.valueOf(rawValue);
				}

				String nopValue = processNopValue(metadataValue, sendAddressLines, mailPiece, sheetCount, pageCount,
						packages);

				if (!insertedNops.contains(key)) {
					AfpRec newRec;
					try {
						newRec = new AfpNopGenericZD(key, nopValue).toAfpRec((short) 0, 0);
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
						throw new StatementCustomException(e.getMessage());
					}
					nopsToInsert.add(newRec);
					insertedNops.add(key);
				}
			}
		}

		for (int j = 0; j < afpRecList.size(); j++) {
			AfpRec rec = afpRecList.get(j);
			if (rec != null && "EDT".equalsIgnoreCase(rec.getTla().trim())) {
				for (int k = nopsToInsert.size() - 1; k >= 0; k--) {
					afpRecList.add(j + 1, nopsToInsert.get(k));
				}
				break;
			}
		}

		return afpRecList;
	}

	private String processNopValue(String nopValue, JSONArray sendAddressLines, JSONObject mailPiece, int sheetCount,
			int pageCount, long packages) throws StatementNonFatalException, StatementCustomException, JSONException {

		// Handle FROM_DERIVEDDATA
		Pattern derivedPattern = Pattern.compile("FROM_DERIVEDDATA\\^([^\\^]+)\\^");
		Matcher derivedMatcher = derivedPattern.matcher(nopValue);
		StringBuffer derivedBuffer = new StringBuffer();
		while (derivedMatcher.find()) {
			String varName = derivedMatcher.group(1);
			String replacement = getDynamicDerivedDataValue(varName, sheetCount, pageCount, packages);
			if (replacement == null)
				replacement = "";
			derivedMatcher.appendReplacement(derivedBuffer, Matcher.quoteReplacement(replacement));
		}
		derivedMatcher.appendTail(derivedBuffer);
		nopValue = derivedBuffer.toString();

		// Handle address line replacements
		nopValue = getAddressLineFromMetadata(sendAddressLines, nopValue);

		// Handle FROM_METADATA
		Pattern metaPattern = Pattern.compile("FROM_METADATA\\^([^\\^]+)\\^");
		Matcher metaMatcher = metaPattern.matcher(nopValue);
		StringBuffer metaBuffer = new StringBuffer();
		while (metaMatcher.find()) {
			String varName = metaMatcher.group(1);
			String replacement = getValueFromMailPiece(mailPiece, varName);
			if (replacement == null)
				replacement = "";
			metaMatcher.appendReplacement(metaBuffer, Matcher.quoteReplacement(replacement));
		}
		metaMatcher.appendTail(metaBuffer);

		return metaBuffer.toString();
	}

	private String resolveDynamicPlaceholdersWithSendAddressOverride(String rawValue, JSONObject mailPiece)
			throws StatementNonFatalException, StatementCustomException, JSONException {
		Matcher matcher = SEND_ADDRESS_LINE_PATTERN.matcher(rawValue);
		JSONArray addressArray = mailPiece.optJSONArray(SEND_ADDRESS_LINES);

		while (matcher.find()) {
			int lineIndex = Integer.parseInt(matcher.group(1)) - 1;
			String replacement = "";
			if (addressArray != null && lineIndex >= 0 && lineIndex < addressArray.length()) {
				replacement = addressArray.optString(lineIndex, "");
			}
			rawValue = matcher.replaceFirst(Matcher.quoteReplacement(replacement));
			matcher = SEND_ADDRESS_LINE_PATTERN.matcher(rawValue); // Allow multiple matches
		}

		return resolveDynamicPlaceholders(rawValue, mailPiece);
	}

	private String getDynamicDerivedDataValue(String field, int sheetCount, int pageCount, long packages) {
		switch (field.toLowerCase()) {
		case "filenumpages":
			return String.valueOf(pageCount);
		case "filenumsheets":
			return String.valueOf(sheetCount);
		case "filenumpackages":
			return String.valueOf(packages);
		default:
			return null;
		}
	}

	private Object getDynamicCmdLineValue(String field) {
		if (this.cmdLine == null) {
			return null;
		}

		switch (field.toLowerCase()) {
		case "rundate":
			return this.cmdLine.getRundate();
		case "corp":
			return this.cmdLine.getCorp() != null ? this.cmdLine.getCorp() : "UNKNOWN";
		case "cycle":
			return this.cmdLine.getCycle() != null ? this.cmdLine.getCycle() : "DEFAULT_CYCLE";
		default:
			return null;
		}
	}

	private void extractedRemoveMethod(MyRawData rawData, List<FileCoordinate> fileCoordinates)
			throws StatementNonFatalException, StatementCustomException, UnsupportedEncodingException {

		List<AfpRec> statementRecords = rawData.statement.getStatementRecords(); // NEW flat list

		for (FileCoordinate fileCoordinate : fileCoordinates) {
			textSpecification.removeTextAt(statementRecords, fileCoordinate);
		}
	}

	private void insertIPOAfterEAG(Statement stmt) throws StatementCustomException, IOException {
		List<String> errors = new ArrayList<>();
		if (isNotEmpty(DataPrepConfig.get().getINSERT_OVERLAYS())) {
			for (Map<String, Object> overlay : DataPrepConfig.get().getINSERT_OVERLAYS()) {
				int pageNum = (int) overlay.get("page_num");
				double x_offset = (double) overlay.get("x_offset");
				double y_offset = (double) overlay.get("y_offset");
				String overlay_name = (String) overlay.get("overlay_name");
				AfpRec iporec = createIPORec(overlay_name, x_offset, y_offset, stmt.getStatementRecords(), pageNum);
				int pageCount = getTotalPagesCount(stmt.getStatementRecords());
				if (pageNum <= 0 || pageNum > pageCount) {
					continue;
				}
				int indexToInsertIPORec = stmt.getIndexForAfpRec(pageNum, "EAG");
				if (indexToInsertIPORec > 0) {
					stmt.getStatementRecords().add(indexToInsertIPORec + 1, iporec);
				} else {
					errors.add("EAG record not found on page " + pageNum + " for overlay: " + overlay_name);
				}
			}
			if (!errors.isEmpty()) {
				throw new StatementCustomException("Errors during IPO insertion:\n" + String.join("\n", errors));
			}
		}
	}

	private void insertNewPages(Statement stmt) throws StatementCustomException, IOException {
		List<Map<String, JsonElement>> elements = DataPrepConfig.get().getINSERT_NEW_PAGES();

		final double[] pageDPI = { 1400 };
		if (elements != null && elements.size() > 0) {
			// Mandatory fields to check
			Set<String> mandatoryFields = Set.of("page_num", "bpg_rec", "bag_rec", "pgd_rec", "ptd_rec", "eag_rec",
					"epg_rec");

			for (Map<String, JsonElement> element : elements) {
				for (String field : mandatoryFields) {
					if (!element.containsKey(field)) {
						throw new StatementCustomException("Missing mandatory field in INSERT_NEW_PAGES: " + field);
					}
				}
			}
			elements = new ArrayList<>(DataPrepConfig.get().getINSERT_NEW_PAGES());
			Function<Map<String, JsonElement>, List<AfpRec>> createNewPage = mp -> {
				List<AfpRec> records = new ArrayList<>();
				Consumer<AfpCmd> addToRecords = cmdRec -> {
					try {
						if (cmdRec != null) {
							AfpRec rec = cmdRec.toAfpRec((short) 0, 0);
							records.add(rec);
						}
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
						throw new RuntimeException(e.getMessage());
					}
				};
				mp.forEach((key, value) -> {
					NewPageRecs pageRec = key.equalsIgnoreCase(PAGE_NUM) ? null
							: NewPageRecs.valueOf(key.toUpperCase());
					AfpCmd afpCMD = null;
					Function<JsonElement, String> defaultValue = jsonElm -> {
						if (jsonElm == null || jsonElm == JsonNull.INSTANCE)
							return "";
						return jsonElm.getAsString();
					};
					if (pageRec != null) {
						switch (pageRec) {
						case BPG_REC:
							String bpgRec = CommonUtils.trimAndPad(value.getAsString());
							afpCMD = new AfpCmdBPG(bpgRec);
							break;
						case BPT_REC:
							String bptRec = CommonUtils.trimAndPad(defaultValue.apply(value));
							afpCMD = new AfpCmdBPT(bptRec);
							break;
						case EPT_REC:
							String eptRec = CommonUtils.trimAndPad(defaultValue.apply(value));
							afpCMD = new AfpCmdEPT(eptRec);
							break;
						case BAG_REC:
							afpCMD = new AfpCmdBAG(defaultValue.apply(value));
							break;
						case EAG_REC:
							afpCMD = new AfpCmdEAG(defaultValue.apply(value));
							break;
						case EPG_REC:
							String epgRec = CommonUtils.trimAndPad(value.getAsString());
							afpCMD = new AfpCmdEPG(epgRec);
							break;
						case IMM_REC:
							String immRec = CommonUtils.trimAndPad(value.getAsString());
							afpCMD = new AfpCmdIMM(immRec);
							break;
						case IPS_RECS:
							JsonArray ipsRecs = value.getAsJsonArray();
							for (int k = 0; k < ipsRecs.size(); k++) {
								JsonObject ipsJSON = ipsRecs.get(k).getAsJsonObject();
								List<String> missingFields = new ArrayList<>();
								if (!ipsJSON.has("pseg_name"))
									missingFields.add("pseg_name");
								if (!ipsJSON.has("x_offset"))
									missingFields.add("x_offset");
								if (!ipsJSON.has("y_offset"))
									missingFields.add("y_offset");
								if (!missingFields.isEmpty()) {
									msger.printMsg("Missing required field(s) while creating IPS record at index " + k
											+ " from ips_recs in INSERT_NEW_PAGES: "
											+ String.join(", ", missingFields));
								}
								String pseg_name = ipsJSON.get("pseg_name").getAsString();
								double x_offset = ipsJSON.get("x_offset").getAsDouble();
								double y_offset = ipsJSON.get("y_offset").getAsDouble();
								SBIN3 xOffset = new SBIN3(inchToDP(x_offset, pageDPI[0]));
								SBIN3 yOffset = new SBIN3(inchToDP(y_offset, pageDPI[0]));
								String psegName = CommonUtils.trimAndPad(pseg_name);
								AfpCmd cmd = new AfpCmdIPS(psegName, xOffset, yOffset);
								addToRecords.accept(cmd);
							}
							break;
						case MCF_RECS:
							JsonArray mcfArray = value.getAsJsonArray();
							mcfArray.forEach(mcf -> {
								JsonObject ob = mcf.getAsJsonObject();
								List<String> missingFields = new ArrayList<>();
								if (!ob.has("id"))
									missingFields.add("id");
								if (!ob.has("coded_font_name"))
									missingFields.add("coded_font_name");
								if (!ob.has("character_set"))
									missingFields.add("character_set");
								if (!ob.has("code_page"))
									missingFields.add("code_page");

								if (!missingFields.isEmpty()) {
									msger.printMsg(
											"Missing required field(s) while creating MCF record from mcf_recs in INSERT_NEW_PAGES: "
													+ String.join(", ", missingFields));
								}
								int id = ob.get("id").getAsInt();
								String coded_font_name = getJSONStringValue(ob, "coded_font_name");
								String character_set = getJSONStringValue(ob, "character_set");
								String code_page = getJSONStringValue(ob, "code_page");
								addToRecords
										.accept(addCmdMCF(character_set, code_page, coded_font_name, new UBIN1(id)));

							});
							break;
						case PGD_REC: {
							JsonObject pgdJSON = value.getAsJsonObject();
							List<String> missingFields = new ArrayList<>();
							if (!pgdJSON.has("width"))
								missingFields.add("width");
							if (!pgdJSON.has("height"))
								missingFields.add("height");
							if (!pgdJSON.has("dot_per_inches"))
								missingFields.add("dot_per_inches");

							if (!missingFields.isEmpty()) {
								msger.printMsg(
										"Missing required field(s) while creating pgd_rec record for INSERT_NEW_PAGES: "
												+ String.join(", ", missingFields));
							}

							if (pgdJSON.has("dot_per_inches") && !pgdJSON.get("dot_per_inches").isJsonNull()) {
								JsonElement dpiElm = pgdJSON.get("dot_per_inches");
								if (!dpiElm.isJsonPrimitive() || !dpiElm.getAsJsonPrimitive().isNumber()) {
									msger.printMsg("FATAL", "dot_per_inches must be numeric");
								}
								pageDPI[0] = dpiElm.getAsDouble();
								if (pageDPI[0] <= 0) {
									msger.printMsg("FATAL", "dot_per_inches must be > 0");
								}
							}

							double width = pgdJSON.get("width").getAsDouble();
							double height = pgdJSON.get("height").getAsDouble();

							UBIN1 xpgbase = new UBIN1(0);
							UBIN1 ypgbase = new UBIN1(0);
							UBIN2 xpgunits = new UBIN2((int) pageDPI[0] * 10);
							UBIN2 ypgunits = new UBIN2((int) pageDPI[0] * 10);
							UBIN3 xpgsize = new UBIN3(inchToDP(width, pageDPI[0]));
							UBIN3 ypgsize = new UBIN3(inchToDP(height, pageDPI[0]));

							AfpByteArrayList bodyBytesList = new AfpByteArrayList();
							bodyBytesList.add(xpgbase.toBytes());
							bodyBytesList.add(ypgbase.toBytes());
							bodyBytesList.add(xpgunits.toBytes());
							bodyBytesList.add(ypgunits.toBytes());
							bodyBytesList.add(xpgsize.toBytes());
							bodyBytesList.add(ypgsize.toBytes());
							bodyBytesList.add(xpgbase.toBytes());
							bodyBytesList.add(xpgbase.toBytes());
							bodyBytesList.add(xpgbase.toBytes());

							byte[] bodyBytes = bodyBytesList.toBytes();
							AfpSFIntro sfIntro = new AfpSFIntro(24, 13870767, (short) 0, 0);
							AfpRec pgdRawRec = new AfpRec((byte) 90, sfIntro, bodyBytes);
							AfpCmdRaw pgdCmdRaw = new AfpCmdRaw(pgdRawRec);
							try {
								afpCMD = new AfpCmdPGD(pgdCmdRaw);
							} catch (ParseException e) {
								throw new RuntimeException(e.getMessage());
							}
							break;
						}

						case PTD_REC: {
							JsonObject ptdJSON = value.getAsJsonObject();
							List<String> missingFields = new ArrayList<>();

							if (!ptdJSON.has("width"))
								missingFields.add("width");
							if (!ptdJSON.has("height"))
								missingFields.add("height");

							if (!missingFields.isEmpty()) {
								msger.printMsg(
										"Missing required field(s) while creating ptd_rec record for INSERT_NEW_PAGES: "
												+ String.join(", ", missingFields));
							}

							UBIN1 xpbase = new UBIN1(0);
							UBIN1 ypbase = new UBIN1(0);
							UBIN2 xpunitvl = new UBIN2((int) pageDPI[0] * 10);
							UBIN2 ypunitvl = new UBIN2((int) pageDPI[0] * 10);
							UBIN3 xpextent = new UBIN3(inchToDP(ptdJSON.get("width").getAsDouble(), pageDPI[0]));
							UBIN3 ypextent = new UBIN3(inchToDP(ptdJSON.get("height").getAsDouble(), pageDPI[0]));
							UBIN2 textflags = new UBIN2(0);

							AfpByteArrayList bodyBytesList = new AfpByteArrayList();
							bodyBytesList.add(xpbase.toBytes());
							bodyBytesList.add(ypbase.toBytes());
							bodyBytesList.add(xpunitvl.toBytes());
							bodyBytesList.add(ypunitvl.toBytes());
							bodyBytesList.add(xpextent.toBytes());
							bodyBytesList.add(ypextent.toBytes());
							bodyBytesList.add(textflags.toBytes());
							bodyBytesList.add("".getBytes());

							byte[] bodyBytes = bodyBytesList.toBytes();
							AfpSFIntro sfIntro = new AfpSFIntro(9, 13873563, (short) 0, 0);
							AfpRec ptdRawRec = new AfpRec((byte) 90, sfIntro, bodyBytes);
							AfpCmdRaw ptdCmdRaw = new AfpCmdRaw(ptdRawRec);
							try {
								afpCMD = new AfpCmdPTD(ptdCmdRaw);
							} catch (ParseException e) {
								throw new RuntimeException(e.getMessage());
							}
							break;
						}
						case IPO_RECS:
							JsonArray ipoArray = value.getAsJsonArray();
							ipoArray.forEach(ipoJSON -> {

								JsonObject ipoObj = ipoJSON.getAsJsonObject();
								List<String> missingFields = new ArrayList<>();
								if (!ipoObj.has("overlay_name"))
									missingFields.add("overlay_name");
								if (!ipoObj.has("x_offset"))
									missingFields.add("x_offset");
								if (!ipoObj.has("y_offset"))
									missingFields.add("y_offset");

								if (!missingFields.isEmpty()) {
									msger.printMsg(
											"Missing required field(s) while creating ipo_recs record for INSERT_NEW_PAGES: "
													+ String.join(", ", missingFields));
								}

								String overlay_name = ipoJSON.getAsJsonObject().get("overlay_name").getAsString();
								double x_offset = ipoJSON.getAsJsonObject().get("x_offset").getAsDouble();
								double y_offset = ipoJSON.getAsJsonObject().get("y_offset").getAsDouble();
								try {
									addToRecords
											.accept(addCmdIPO(overlay_name, new SBIN3(inchToDP(x_offset, pageDPI[0])),
													new SBIN3(inchToDP(y_offset, pageDPI[0]))));
								} catch (StatementCustomException e) {
									e.printStackTrace();
									throw new RuntimeException(e.getMessage());
								}
							});
							break;
						case PTX_RECS:
							JsonArray ptxArray = value.getAsJsonArray();
							ptxArray.forEach(ptxJSON -> {
								JsonObject ptxObj = ptxJSON.getAsJsonObject();
								short x_offset;
								short y_offset;
								List<String> missingFields = new ArrayList<>();
								if (!ptxObj.has("x_offset"))
									missingFields.add("x_offset");
								if (!ptxObj.has("y_offset"))
									missingFields.add("y_offset");
								if (!ptxObj.has("orientation"))
									missingFields.add("orientation");
								if (!ptxObj.has("font_id"))
									missingFields.add("font_id");
								if (!ptxObj.has("trn_text"))
									missingFields.add("trn_text");

								if (!missingFields.isEmpty()) {
									msger.printMsg(
											"Missing required field(s) while creating ptx_recs record for INSERT_NEW_PAGES: "
													+ String.join(", ", missingFields));
								}

								try {
									x_offset = numToDP(ptxJSON.getAsJsonObject().get("x_offset").getAsDouble(),
											pageDPI[0]);
									y_offset = numToDP(ptxJSON.getAsJsonObject().get("y_offset").getAsDouble(),
											pageDPI[0]);
								} catch (StatementCustomException e) {
									e.printStackTrace();
									throw new RuntimeException(e.getMessage());
								}
								short font_id = ptxJSON.getAsJsonObject().get("font_id").getAsShort();
								String trn_text = ptxJSON.getAsJsonObject().get("trn_text").getAsString();
								int orientation = ptxJSON.getAsJsonObject().get("orientation").getAsInt();
								List<Integer> validOrientations = Arrays.asList(0, 90, 180, 270);
								if (!validOrientations.contains(orientation)) {
									msger.printMsg("FATAL",
											"Invalid orientation value in configuration for insert new pages: "
													+ orientation + " (allowed values: 0, 90, 180, 270)");
								}
								JSONArray sendAddressLines = stmt.getMetaData().optJSONArray(SEND_ADDRESS_LINES);
								if (trn_text.contains("send_address_line"))
									try {
										trn_text = getAddressLineFromMetadata(sendAddressLines, trn_text);
									} catch (StatementCustomException e) {
										e.printStackTrace();
										throw new RuntimeException(e.getMessage());
									}
								else if (trn_text.contains("FROM_METADATA")) {
									Function<String, String> extractMetadataKey = input -> {
										String extractedWord = "";
										String start = "FROM_METADATA^";
										String end = "^";
										int startIndex = input.indexOf(start);
										int endIndex = input.indexOf(end, startIndex + start.length());
										if (startIndex != -1 && endIndex != -1)
											extractedWord = input.substring(startIndex, endIndex + end.length());
										return extractedWord;
									};
									String extractedKey = extractMetadataKey.apply(trn_text);
									String metadata_key = extractedKey.split("\\^")[1];
									Object rawValue = stmt.getMetaData().opt(metadata_key);
									String metadata_value;

									if (rawValue == null || JSONObject.NULL.equals(rawValue)) {
										metadata_value = "";
									} else if (rawValue instanceof Number) {
										double val = ((Number) rawValue).doubleValue();
										if (val == Math.floor(val)) {
											metadata_value = String.valueOf((long) val);
										} else {
											metadata_value = String.valueOf(val);
										}
									} else {
										metadata_value = String.valueOf(rawValue);
									}

									trn_text = trn_text.replace(extractedKey, metadata_value);
								}
								addToRecords.accept(createPTXRecord(x_offset, y_offset, trn_text, font_id));
							});
							break;
						default:
							break;
						}
						addToRecords.accept(afpCMD);
					}
				});
				return records;
			};
			Collections.reverse(elements);
			elements.forEach(mp -> {
				int pageNum = mp.get(PAGE_NUM).getAsInt();
				List<AfpRec> newPage = createNewPage.apply(mp);
				stmt.insertPageAt(newPage, pageNum);
			});
		}
	}

	public List<AfpRec> insertDocInsertComboTle(JSONObject mailPiece, Statement stmt,
			List<Map<String, Integer>> hopperLookup, Set<String> insertedTles)
			throws StatementCustomException, JSONException {

		List<String> insertIds = new ArrayList<>();

		if (!mailPiece.has("insert_ids")) {
			throw new StatementCustomException("Missing required field 'insert_ids' in metadata JSON.");
		}

		JSONArray insertIdsJson = mailPiece.optJSONArray("insert_ids");
		if (insertIdsJson != null) {
			for (int i = 0; i < insertIdsJson.length(); i++) {
				Object value = insertIdsJson.get(i);
				insertIds.add((value == null || JSONObject.NULL.equals(value)) ? "" : value.toString());
			}
		} else {
			msger.logDebug("Insert IDs from metadata is null");
		}

		String insertCombo = generateDocInsertCombo(insertIds, hopperLookup);
		String tleKey = "DOC_INSERT_COMBO:" + insertCombo;

		if (!insertedTles.contains(tleKey)) {
			AfpRec comboTleRecord;
			try {
				comboTleRecord = new AfpCmdTLE("DOC_INSERT_COMBO", insertCombo).toAfpRec((short) 0, 0);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				throw new StatementCustomException("Error creating DOC_INSERT_COMBO TLE");
			}

			List<AfpRec> headerRecords = extractStatementHeader(stmt);

			headerRecords.add(comboTleRecord);

			List<AfpRec> mergedRecords = new ArrayList<>(headerRecords);
			boolean insideBody = false;
			for (AfpRec record : stmt.getStatementRecords()) {
				if ("BPG".equals(record.getTla())) {
					insideBody = true;
				}
				if (insideBody) {
					mergedRecords.add(record);
				}
			}

			stmt.setStatementRecords(mergedRecords);

			insertedTles.add(tleKey);
		}

		return stmt.getStatementRecords();
	}

	public static String generateDocInsertCombo(List<String> insertIds, List<Map<String, Integer>> hopperLookupList)
			throws StatementCustomException {

		Map<String, Integer> lookupMap = new HashMap<>();
		msger.logDebug("hopperLookupList => " + hopperLookupList);

		for (Map<String, Integer> map : hopperLookupList) {
			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				int pos = entry.getValue();
				if (pos < 1 || pos > 11) {
					throw new StatementCustomException(
							"Invalid hopper number: " + pos + " for insert_id: " + entry.getKey());
				}
				lookupMap.put(entry.getKey(), pos);
			}
		}

		char[] combo = "NNNNNNNNNNN".toCharArray();

		for (String insertId : insertIds) {
			if (insertId == null || insertId.isEmpty()) {
				continue;
			}
			Integer pos = lookupMap.get(insertId);
			if (pos != null) {
				combo[pos - 1] = 'Y';
			}
		}

		return new String(combo);
	}

	private List<AfpRec> setIMMRecToPage(List<AfpRec> updatedStmtPages, Map<String, AfpRec> immAfpMap)
			throws StatementCustomException {
		int bpgCounter = 0;
		List<AfpRec> modifiedStmtPages = new ArrayList<>();
		String prevRec = "";
		for (AfpRec page : updatedStmtPages) {
			if (page != null && "BPG".equals(page.getTla())) {
				bpgCounter++;

				if (immAfpMap != null && immAfpMap.containsKey(String.valueOf(bpgCounter))) {
					AfpRec immRecord = immAfpMap.get(String.valueOf(bpgCounter));

					if (immRecord != null && !prevRec.equalsIgnoreCase("IMM")) {
						if (bpgCounter == 1) {
							msger.printMsg("FATAL", "IMM for Page 1 is not set by default");
						}
						modifiedStmtPages.add(immRecord);
					}
				}
			}
			try {
				if (page != null && "IMM".equals(page.getTLEName())) {
					prevRec = page.getTLEName();
				}
			} catch (ParseException e) {
				e.printStackTrace();
				throw new StatementCustomException(e.getMessage());
			}

			modifiedStmtPages.add(page);
		}

		return modifiedStmtPages;
	}

	private Map<String, AfpRec> addIMMForDocument(List<AfpRec> pages, Map<Integer, Integer> newSheets)
			throws StatementCustomException {

		Map<String, AfpRec> immAfpMap = new HashMap<>();
		Collection<Integer> targetPages = newSheets.values();
		int pageIndex = 0;
		String currentIMM_MMPName = "";

		for (AfpRec page : pages) {
			pageIndex++;
			if (targetPages.contains(pageIndex)) {
				AfpCmdIMM afpCmdImm = new AfpCmdIMM(String.format("%-8s", currentIMM_MMPName));
				try {
					AfpRec immRecord = afpCmdImm.toAfpRec((short) 0, 0);
					if (immRecord != null) {
						immAfpMap.put(Integer.toString(pageIndex), immRecord);
					}
				} catch (UnsupportedEncodingException e) {
					throw new StatementCustomException("Error generating IMM for page " + pageIndex, e);
				}
			}
			try {
				if (page != null && "IMM".equals(page.getTLEName())) {
					Pattern pattern = Pattern.compile("MMPName\\s*:\\s*\"(.*?)\"");
					Matcher matcher = pattern.matcher(page.toEnglish(null));
					if (matcher.find()) {
						currentIMM_MMPName = matcher.group(1);
					} else {
						msger.logDebug("MMPName not found at page " + pageIndex);
					}
				}
			} catch (UnsupportedEncodingException | ParseException e) {
				throw new StatementCustomException("Error parsing IMM on page " + pageIndex, e);
			}
		}

		return immAfpMap;
	}

	private HashMap<Integer, Integer> getNewSheetPages(MyRawData rawData) throws IOException, StatementCustomException {
		int nextStartPage = 1;
		int newPageIndex = 0;
		HashMap<Integer, Integer> newSheetPages = new HashMap<>();
		try {
			JSONArray pdfDocuments = rawData.statement.getMetaData().getJSONArray("pdf_documents");
			msger.logDebug("Total PDF documents" + pdfDocuments.length());

			for (int j = 0; j < pdfDocuments.length(); j++) {
				newPageIndex++;
				JSONObject pdfDocument = pdfDocuments.getJSONObject(j);
				msger.logDebug("New Sheet:" + newPageIndex + ", on Page" + nextStartPage);
				int pageCount = pdfDocument.getInt("page_count");
				newSheetPages.put(newPageIndex, nextStartPage);
				nextStartPage = pageCount + 1;
			}

		} catch (JSONException e) {
			msger.printMsg("Error processing JSON: " + e.getMessage());
			throw new StatementCustomException(e.getMessage());
		}
		return newSheetPages;
	}

	public String lookupSpecialHandlingCode(JSONObject mailPiece, List<Map<String, Object>> config)
			throws StatementCustomException {

		if (config == null || config.isEmpty()) {
			return null;
		}

		if (!mailPiece.has("shipping_carrier") || !mailPiece.has("shipping_service")) {
			throw new StatementCustomException(
					"Missing required metadata fields: shipping_carrier and/or shipping_service");
		}

		String carrier = Optional.ofNullable(mailPiece.opt("shipping_carrier")).map(Object::toString).orElse("").trim();

		String service = Optional.ofNullable(mailPiece.opt("shipping_service")).map(Object::toString).orElse("").trim();

		msger.logDebug("Looking up SH code for carrier: '" + carrier + "', service: '" + service + "'");

		for (Map<String, Object> entry : config) {
			String configCarrier = Optional.ofNullable((String) entry.get("carrier")).orElse("").trim();
			String configService = Optional.ofNullable((String) entry.get("service")).orElse("").trim();

			if (wildcardMatch(carrier, configCarrier) && wildcardMatch(service, configService)) {
				msger.logDebug("Matched config: " + entry);
				return String.valueOf(entry.get("sh_code"));
			}
		}

		msger.logDebug("No match found in config.");
		return null;
	}

	// simple wildcard match helper
	private boolean wildcardMatch(String value, String pattern) {
		if ("*".equals(pattern)) {
			return true;
		}
		return value.equalsIgnoreCase(pattern);
	}

	private String getAddressLineFromMetadata(JSONArray sendAddressLines, String nopValue)
			throws StatementCustomException {

		if (sendAddressLines == null) {
			msger.printMsg("Missing 'send_address_lines' in metadata while processing value: " + nopValue);
			throw new StatementCustomException(
					"Missing 'send_address_lines' in metadata while processing value: " + nopValue);
		}

		Pattern pattern = Pattern.compile("FROM_METADATA\\^send_address_line_(\\d+)\\^"); // changed from (\\d)
		Matcher matcher = pattern.matcher(nopValue);
		StringBuffer result = new StringBuffer();

		while (matcher.find()) {
			int lineIndex = Integer.parseInt(matcher.group(1)) - 1;
			String replacement = "";

			if (lineIndex >= 0 && lineIndex < sendAddressLines.length()) {
				replacement = sendAddressLines.optString(lineIndex, "");
			}

			matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
		}

		matcher.appendTail(result);
		return result.toString();
	}

	private void translateImmRecs(Statement statement, List<Map<String, String>> translateImmRecsConfig)
			throws UnsupportedEncodingException, StatementCustomException {

		msger.logDebug("Starting IMM translation with config: " + translateImmRecsConfig);

		Map<String, String> translateMap = new HashMap<>();
		for (Map<String, String> entry : translateImmRecsConfig) {
			translateMap.putAll(entry);
		}
		msger.logDebug("Compiled translation map: " + translateMap);

		boolean errorOccurred = false;
		List<AfpRec> records = statement.getStatementRecords();

		for (int i = 0; i < records.size(); i++) {
			AfpRec rec = records.get(i);

			if (rec != null && "IMM".equals(rec.getTla())) {
				msger.logDebug("Found IMM record at index " + i);

				AfpCmdRaw afpCmdRaw = new AfpCmdRaw(rec);
				if (afpCmdRaw.getCmdType() == AfpCmdType.IMM) {
					AfpCmdIMM immRec;
					try {
						immRec = (AfpCmdIMM) rec.parse();
					} catch (ParseException e) {
						throw new StatementCustomException("Error creating IMM record: " + e.getMessage(), e);
					}

					String currentImmName = immRec.getResources().asString().trim();
					msger.logDebug("Current IMM name: '" + currentImmName + "'");

					if (translateMap.containsKey(currentImmName)) {
						String newImmName = translateMap.get(currentImmName);
						msger.logDebug("Translating IMM: '" + currentImmName + "' to '" + newImmName + "'");

						if (newImmName.length() > 8) {
							msger.printMsg("TRANSLATE_IMM_NAME_EXCEED_LENGTH",
									"Translated IMM name '" + newImmName + "' exceeds 8 characters.");
							errorOccurred = true;
							continue;
						}

						String paddedNewImmName = String.format("%-8s", newImmName);
						AfpCmdIMM newImmRec = new AfpCmdIMM(paddedNewImmName);

						AfpRec updatedRec = newImmRec.toAfpRec((short) 0, 0);

						msger.logDebug("Replacing IMM record with new IMM: '" + paddedNewImmName + "'");
						records.set(i, updatedRec);
					} else {
						msger.logDebug("IMM name '" + currentImmName + "' not found in translation map. Skipping.");
					}
				}
			}
		}

		if (errorOccurred) {
			msger.printMsg("TRANSLATE_IMM_NAME_EXCEED_LENGTH", "IMM translations failed due to length constraints");
			throw new StatementCustomException("IMM translations failed due to length constraints");
		} else {
			msger.logDebug("IMM translation completed successfully.");
		}
	}

	public List<AfpRec> insertFileLevelNamedNops(List<AfpRec> headerRecs, Set<String> insertedNops,
			List<Map<String, String>> insert_FILE_HEADER_NAMED_NOPS, JSONObject mailPiece) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<AfpRec> insertTrailerNamedNops(List<AfpRec> trailerTle, Set<String> insertedNops,
			List<Map<String, String>> insert_FILE_TRAILER_NAMED_NOPS, int totalSheetCount, int totalPageCount,
			JSONObject mailPiece, int totalMailPieces) {
		// TODO Auto-generated method stub
		return null;
	}

	public static List<AfpRec> extractStatementHeader(Statement statement) {
		List<AfpRec> headerRecords = new ArrayList<>();
		for (AfpRec rec : statement.getStatementRecords()) {
			if ("BPG".equals(rec.getTla())) {
				break;
			}
			headerRecords.add(rec);
		}
		return headerRecords;
	}

	public int getTotalPagesCount(List<AfpRec> afpRecs) {
		int count = 0;
		for (AfpRec rec : afpRecs) {
			if ("BPG".equals(rec.getTla()))
				count++;
		}
		return count;
	}
}
