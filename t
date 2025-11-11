{
    "INSERT_FILE_HEADER_NAMED_NOPS" : [
        { "UR_PRODUCT_ID": "PFG/REMAIL" }
    ],

    "SET_IMM_RECS" : [
        { "1": "TRAY01DP" }
    ],
    
    "INSERT_BLANK_PAGE_AFTER_EACH_INPUT_PAGE" : "Y",

    "EACH_PDF_DOCUMENT_STARTS_NEW_SHEET" : "Y",

    "INSERT_ID_HOPPER_LOOKUP" : [
		{ "VV599" : 11 },
		{ "VV9976" : 11 },
		{ "VV1302" : 11 },
		{ "VV10023" : 11 },
		{ "VV934" : 11 },
		{ "VV1434-10" : 11 },
        { "VV10051" : 11 }
	],

    "INSERT_MAIL_PIECE_TLES" : [
        { "DOC_ACCOUNT_NUMBER": "FROM_METADATA^infact_account_number^" },
        { "ACCOUNT_CD": "FROM_METADATA^infact_account_number^" },
        { "DOC_PRODUCT_CODE": "PS" },
        { "PS_DOC_PRODUCT_CODE": "QV,EVSB" },
        { "DOC_SPECIAL_HANDLING_CODE": "99" }
    ]
}
