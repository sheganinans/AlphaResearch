module Geddit.Exchange

type Exchange =
  | AMEX = 4 
  | BARK = 45 
  | BATS = 60 
  | BATY = 63 
  | BOST = 11 
  | BTRF = 58 
  | C2 = 42
  | CBOE = 5 
  | CBOT = 24 
  | CDNX = 19 
  | CFE = 54 
  | CHIC = 17 
  | CINC = 8
  | CLRP = 44 
  | CME = 20 
  | CMEX = 67 
  | COMP = 0 
  | COMX = 23
  | DOWJ = 30 
  | DTN = 36 
  | EDGE = 64 
  | EDGX = 65 
  | ENCM = 51 
  | ENID = 52 
  | ENIR = 53 
  | ENXT = 35 
  | EUEU = 50 
  | EURX = 34 
  | EUUS = 49 
  | FTSE = 33 
  | GEMI = 31 
  | HOTS = 48 
  | HWTB = 56 
  | IEX = 68 
  | IPEX = 39
  | ISEX = 6 
  | KCBT = 26 
  | LME = 38 
  | LMT = 37 
  | LSE = 70 
  | LTSE = 75
  | MEMX = 73 
  | MGEX = 27 
  | MIAX = 43 
  | MRCY = 22 
  | MX = 40 
  | NQAD = 2 
  | NQAG = 16 
  | NQBB = 14 
  | NQBX = 47 
  | NQEX = 1 
  | NQNM = 12
  | NQNX = 57 
  | NQPK = 15 
  | NQSC = 13
  | NTRF = 59 
  | NYBT = 21 
  | NYLF = 61 
  | NYMX = 25 
  | NYSE = 3 
  | NYSE_GIF = 71
  | ONEC = 29 
  | OPRA = 10 
  | PACF = 7
  | PBOT = 55 
  | PERL = 69 
  | PHIL = 9 
  | PINK = 62 
  | RUSL = 66 
  | SIMX = 32 
  | TBA_74 = 74
  | TBA_76 = 76
  | TBA_77 = 77
  | TBA_78 = 78
  | TBA_79 = 79
  | TEN4 = 46 
  | TSE = 18 
  | TSX_IDX = 72
  | WCE = 28 
  | WSE = 41
  
let info : Map<Exchange, string * string> =
  [
    Exchange.AMEX, ("XASE", "American Stock Exchange") 
    Exchange.BARK, ("BARX", "Barclays") 
    Exchange.BATS, ("BATS", "BATS Trading") 
    Exchange.BATY, ("BATY", "BATS Trading") 
    Exchange.BOST, ("XBOS", "Boston Stock/Options Exchange") 
    Exchange.BTRF, ("XADF", "BSE Trade Reporting Facility") 
    Exchange.C2, ("C2OX", "CBOE C2 Option Exchange") 
    Exchange.CBOE, ("XCBO", "Chicago Board Options Exchange") 
    Exchange.CBOT, ("GLBX", "Chicago Board of Trade") 
    Exchange.CDNX, ("XTSX", "Canadian Venture Exchange") 
    Exchange.CFE, ("XCBF", "CBOE Futures Exchange") 
    Exchange.CHIC, ("CXHI", "Chicago Stock Exchange") 
    Exchange.CINC, ("XCIS", "National Stock Exchange (Cincinnati)") 
    Exchange.CLRP, ("XNYM", "NYMEX Clearport") 
    Exchange.CME, ("XCME", "Chicago Mercantile Exchange") 
    Exchange.CMEX, ("XIOM", "CME Indexes") 
    Exchange.COMP, ("", "Comp") 
    Exchange.COMX, ("XCEC", "COMEX (division of NYMEX)") 
    Exchange.DOWJ, ("", "Dow Jones Indicies") 
    Exchange.DTN, ("", "Data Transmission Network") 
    Exchange.EDGE, ("EDGA", "Direct Edge") 
    Exchange.EDGX, ("EDGX", "Direct Edge") 
    Exchange.ENCM, ("XEUC", "Euronext Commodities") 
    Exchange.ENID, ("XEUE", "Euronext Index Derivatives") 
    Exchange.ENIR, ("XEUI", "Euronext Interest Rates") 
    Exchange.ENXT, ("XAMS", "EuroNext") 
    Exchange.EUEU, ("XEUR", "Eurex EU") 
    Exchange.EURX, ("XEUR", "Eurex") 
    Exchange.EUUS, ("XEUR", "Eurex US") 
    Exchange.FTSE, ("XLON", "London Stock Exchange") 
    Exchange.GEMI, ("GMNI", "ISE Gemini") 
    Exchange.HOTS, ("XEUR", "HotSpot Eurex US") 
    Exchange.HWTB, ("XHAN", "Hannover WTB Exchange") 
    Exchange.IEX, ("IEXG", "Investors Exchange") 
    Exchange.IPEX, ("IEPA", "Intercontinental Exchange (IPE)") 
    Exchange.ISEX, ("XISX", "International Securities Exchange") 
    Exchange.KCBT, ("XKBT", "Kansas City Board of Trade") 
    Exchange.LME, ("XLME", "London Metals Exchange") 
    Exchange.LMT, ("XLME", "London Metals Exchange Matched Trades") 
    Exchange.LSE, ("LSE", "London Stock Exchange") 
    Exchange.LTSE, ("", "Long Term Stock Exchange") 
    Exchange.MEMX, ("MEMX", "Members Exchange") 
    Exchange.MGEX, ("XMGE", "Minneapolis Grain Exchange") 
    Exchange.MIAX, ("XMIO", "Miami Exchange") 
    Exchange.MRCY, ("MCRY", "ISE Mercury") 
    Exchange.MX, ("XMOD", "Montreal Stock Exchange") 
    Exchange.NQAD, ("XADF", "Nasdaq Alternative Display Facility") 
    Exchange.NQAG, ("XADF", "Nasdaq Aggregate Quote") 
    Exchange.NQBB, ("OOTC", "Nasdaq Bulletin Board") 
    Exchange.NQBX, ("XBOS", "NASDAQ Boston") 
    Exchange.NQEX, ("XNMS", "Nasdaq Exchange") 
    Exchange.NQNM, ("XNGS", "Nasdaq Global+Select Market (NMS)") 
    Exchange.NQNX, ("FINN", "FINRA/NASDAQ Trade Reporting Facility") 
    Exchange.NQPK, ("OOTC", "Nasdaq OTC") 
    Exchange.NQSC, ("XNCM", "Nasdaq Capital Market (SmallCap)") 
    Exchange.NTRF, ("FINY", "NYSE Trade Reporting Facility") 
    Exchange.NYBT, ("IMAG", "New York Board of Trade") 
    Exchange.NYLF, ("XNLI", "NYSE LIFFE metals contracts") 
    Exchange.NYMX, ("XNYM", "New York Mercantile Exchange") 
    Exchange.NYSE, ("XNYS", "New York Stock Exchange") 
    Exchange.NYSE_GIF, ("NYSE_GIF", "NYSE Global Index Feed") 
    Exchange.ONEC, ("XOCH", "OneChicago Exchange") 
    Exchange.OPRA, ("OPRA", "Options Pricing Reporting Authority") 
    Exchange.PACF, ("ARCX", "NYSE ARCA (Pacific)") 
    Exchange.PBOT, ("XPBT", "Philadelphia Board of Trade") 
    Exchange.PERL, ("PERL", "Miami Pearl Options Exchange") 
    Exchange.PHIL, ("XPHL", "Philidelphia Stock Exchange") 
    Exchange.PINK, ("OTCM", "Pink Sheets") 
    Exchange.RUSL, ("", "Russell Indexes") 
    Exchange.SIMX, ("XSES", "Singapore International Monetary Exchange") 
    Exchange.TBA_74, ("TBA_74", "TBA Exchange 74") 
    Exchange.TBA_76, ("TBA_76", "TBA Exchange 76") 
    Exchange.TBA_77, ("TBA_77", "TBA Exchange 77") 
    Exchange.TBA_78, ("TBA_78", "TBA Exchange 78") 
    Exchange.TBA_79, ("TBA_79", "TBA Exchange 79") 
    Exchange.TEN4, ("", "TenFore") 
    Exchange.TSE, ("XTSE", "Toronto Stock Exchange") 
    Exchange.TSX_IDX, ("TSX_IDX", "TSX Indexes") 
    Exchange.WCE, ("IFCA", "Winnipeg Commodity Exchange") 
    Exchange.WSE, ("XTSX", "Winnipeg Stock Exchange") 
  ] |> Map.ofList