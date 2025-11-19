with regards to the UR change, we removed the "DOC_PRODUCT_CODE" in the print afpds.ps file, but it is still showing up in the UR file and that's what's causing the issue:
 
 
NOP   (D3EEEE, AfpGen No Operation); len 29; rec 409; offset 163,060

    00000 Token                        (ebc t1e4lat1) : "DOC_PRODUCT_CODE"

    00016 Separator                                   : 0x00

    00017 Value                        (ebc t1e4lat1) : ",UR"

    00020 Separator                                   : 0x00
 
this NOP needs to be removed from the UR file
 
