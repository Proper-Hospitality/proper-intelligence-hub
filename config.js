// Property configurations for Proper Hospitality Intelligence Hub
const PROPERTIES = {
  smp: {
    id: 'smp',
    name: 'Santa Monica Proper Hotel',
    shortName: 'Santa Monica Proper',
    code: 'SMP',
    navTitle: ['SMP', 'Intelligence'],
    dataFile: 'data/smp.json',
    outlets: {
      primary: [
        { key: 'Calabra', label: 'Calabra', color: '#C8A951' },
        { key: 'Palma', label: 'Palma', color: '#58A6FF' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' }
      ],
      survey: [
        { key: 'survey_calabra', label: 'Calabra', commentsKey: 'calabra_comments', fields: ['Food Quality','Staff','Service','Menu'] },
        { key: 'survey_surya', label: 'Surya Spa', commentsKey: 'surya_comments', fields: ['Reservations','Therapy Rooms','Therapist','Staff'] },
        { key: 'survey_palma', label: 'Palma & Pool', commentsKey: 'palma_comments', fields: ['Food Quality','Staff','Service','Menu'], secondaryKey: 'survey_pool', secondaryLabel: 'Rooftop Pool', secondaryFields: ['Maintenance','Service','F&B'] }
      ]
    },
    compSetLabel: 'SMP vs Competitors',
    radarLabel: 'SMP Average'
  },
  austin: {
    id: 'austin',
    name: 'Austin Proper Hotel',
    shortName: 'Austin Proper',
    code: 'AUS',
    navTitle: ['Austin Proper', 'Intelligence'],
    dataFile: 'data/austin.json',
    outlets: {
      primary: [
        { key: 'The Peacock', label: 'The Peacock', color: '#C8A951' },
        { key: 'La Piscina', label: 'La Piscina', color: '#58A6FF' },
        { key: "Goldie's", label: "Goldie's", color: '#3FB950' },
        { key: 'Kappo Kappo', label: 'Kappo Kappo', color: '#BC8CFF' },
        { key: 'Quill Room', label: 'Quill Room', color: '#D29922' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#F85149' }
      ],
      survey: [
        { key: 'survey_surya', label: 'Spa', commentsKey: 'surya_comments', fields: ['Reservations','Therapy Rooms','Therapist','Staff'] }
      ]
    },
    compSetLabel: 'Austin Proper vs Competitors',
    radarLabel: 'Austin Proper Average'
  },
  shelborne: {
    id: 'shelborne',
    name: 'The Shelborne By Proper',
    shortName: 'Shelborne',
    code: 'SHL',
    navTitle: ['Shelborne', 'Intelligence'],
    dataFile: 'data/shelborne.json',
    outlets: {
      primary: [
        { key: 'Pauline', label: 'Pauline', color: '#C8A951' },
        { key: 'Little Torch', label: 'Little Torch', color: '#58A6FF' },
        { key: 'The Café', label: 'The Café', color: '#3FB950' },
        { key: 'Terrace', label: 'Terrace', color: '#D29922' },
        { key: 'The Pool & Beach Club', label: 'Pool & Beach Club', color: '#F85149' },
        { key: 'The Bar', label: 'The Bar', color: '#BC8CFF' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#79C0FF' }
      ],
      survey: []
    },
    compSetLabel: 'Shelborne vs Competitors',
    radarLabel: 'Shelborne Average'
  },
  sf_proper: {
    id: 'sf_proper',
    name: 'San Francisco Proper Hotel',
    shortName: 'SF Proper',
    code: 'SFP',
    navTitle: ['SF Proper', 'Intelligence'],
    dataFile: 'data/sf_proper.json',
    outlets: {
      primary: [
        { key: 'Villon', label: 'Villon', color: '#C8A951' },
        { key: "Charmaine's", label: "Charmaine's", color: '#58A6FF' },
        { key: "Gilda's", label: "Gilda's", color: '#3FB950' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' }
      ],
      survey: []
    },
    compSetLabel: 'SF Proper vs Competitors',
    radarLabel: 'SF Proper Average'
  },
  dtla: {
    id: 'dtla',
    name: 'Proper Downtown Los Angeles',
    shortName: 'DTLA Proper',
    code: 'DTLA',
    navTitle: ['DTLA Proper', 'Intelligence'],
    dataFile: 'data/dtla.json',
    outlets: {
      primary: [
        { key: 'Cara Cara', label: 'Cara Cara', color: '#C8A951' },
        { key: 'Cabrillo', label: 'Cabrillo', color: '#58A6FF' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' }
      ],
      survey: []
    },
    compSetLabel: 'DTLA Proper vs Competitors',
    radarLabel: 'DTLA Proper Average'
  },
  hotel_june: {
    id: 'hotel_june',
    name: 'Hotel June West LA',
    shortName: 'Hotel June',
    code: 'JUNE',
    navTitle: ['Hotel June', 'Intelligence'],
    dataFile: 'data/hotel_june.json',
    outlets: {
      primary: [
        { key: 'Scenic Route', label: 'Scenic Route', color: '#C8A951' },
        { key: 'Caravan Swim Club', label: 'Caravan Swim Club', color: '#58A6FF' }
      ],
      survey: []
    },
    compSetLabel: 'Hotel June vs Competitors',
    radarLabel: 'Hotel June Average'
  },
  june_malibu: {
    id: 'june_malibu',
    name: 'Hotel June Malibu',
    shortName: 'June Malibu',
    code: 'JUNM',
    navTitle: ['June Malibu', 'Intelligence'],
    dataFile: 'data/june_malibu.json',
    outlets: {
      primary: [],
      survey: []
    },
    compSetLabel: 'June Malibu vs Competitors',
    radarLabel: 'June Malibu Average'
  },
  avalon_ps: {
    id: 'avalon_ps',
    name: 'Avalon Hotel and Bungalows Palm Springs',
    shortName: 'Avalon Palm Springs',
    code: 'APS',
    navTitle: ['Avalon PS', 'Intelligence'],
    dataFile: 'data/avalon_ps.json',
    outlets: {
      primary: [
        { key: 'Chi Chi', label: 'Chi Chi', color: '#C8A951' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' }
      ],
      survey: []
    },
    compSetLabel: 'Avalon PS vs Competitors',
    radarLabel: 'Avalon PS Average'
  },
  avalon_bh: {
    id: 'avalon_bh',
    name: 'Avalon Hotel Beverly Hills',
    shortName: 'Avalon Beverly Hills',
    code: 'ABH',
    navTitle: ['Avalon BH', 'Intelligence'],
    dataFile: 'data/avalon_bh.json',
    outlets: {
      primary: [
        { key: 'Viviane', label: 'Viviane', color: '#C8A951' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' }
      ],
      survey: []
    },
    compSetLabel: 'Avalon BH vs Competitors',
    radarLabel: 'Avalon BH Average'
  },
  culver: {
    id: 'culver',
    name: 'The Culver Hotel',
    shortName: 'Culver Hotel',
    code: 'CLV',
    navTitle: ['Culver Hotel', 'Intelligence'],
    dataFile: 'data/culver.json',
    outlets: {
      primary: [
        { key: 'The Culver Hotel Restaurant & Bar', label: 'Restaurant & Bar', color: '#C8A951' },
        { key: 'Velvet', label: 'Velvet', color: '#58A6FF' }
      ],
      survey: []
    },
    compSetLabel: 'Culver Hotel vs Competitors',
    radarLabel: 'Culver Hotel Average'
  },
  montauk: {
    id: 'montauk',
    name: 'Montauk Yacht Club',
    shortName: 'Montauk YC',
    code: 'MYC',
    navTitle: ['Montauk YC', 'Intelligence'],
    dataFile: 'data/montauk.json',
    outlets: {
      primary: [
        { key: 'Ocean Club', label: 'Ocean Club', color: '#C8A951' },
        { key: 'Grab & Go', label: 'Grab & Go', color: '#58A6FF' }
      ],
      survey: []
    },
    compSetLabel: 'Montauk YC vs Competitors',
    radarLabel: 'Montauk YC Average'
  },
  ingleside: {
    id: 'ingleside',
    name: 'Ingleside Estate',
    shortName: 'Ingleside',
    code: 'ING',
    navTitle: ['Ingleside', 'Intelligence'],
    dataFile: 'data/ingleside.json',
    outlets: {
      primary: [
        { key: "Melvyn's", label: "Melvyn's", color: '#C8A951' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' }
      ],
      survey: []
    },
    compSetLabel: 'Ingleside vs Competitors',
    radarLabel: 'Ingleside Average'
  }
};

// All properties in portfolio order (by revenue, descending)
const PORTFOLIO_ORDER = ['shelborne', 'austin', 'smp', 'hotel_june', 'dtla', 'sf_proper', 'avalon_ps', 'montauk', 'avalon_bh', 'ingleside', 'culver', 'june_malibu'];
