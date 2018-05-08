(function () {
  // const DB_NAMES = [];
  const TB_NAMES = [
    'raw_sample', 'ad_feature', 'user_profile', 'behavior_log'
  ];
  const COL_NAMES = {
    ad_feature: [
      ['adgroup_id', 'Ad group ID'],
      ['cate_id', 'Category ID'],
      ['campaign_id', 'Campaign ID'],
      ['brand', 'Brand'],
      ['customer_id', 'Customer ID'],
      ['price', 'Price']
    ],
    user_profile: [
      ['userid', 'User ID'],
      ['final_gender_code', 'Gender'],
      ['age_level', 'Age level'],
      ['pvalue_level', 'Consumption Grade'],
      ['shopping_level', 'Shopping Level'],
      ['occupation', 'Occupation'],
      ['new_user_class_level', 'City level']
    ],
    behavior_log: [
      ['nick', 'User ID'],
      ['time_stamp', 'Timestamp'],
      ['btag', 'Behavior Type']
    ]
  };
  const AG_NAMES = [
    'max', 'min', 'avg',
    'count', 'count_unique', 'show_columns'
  ];

  function replaceSelection(event) {
    var oldSelection = document.querySelector('#col-select');
    var columnParent = oldSelection.closest('div.form-group');
    var tableSelection = document.querySelector('#ds-select');
    var curTableName = tableSelection.value;
    var options = COL_NAMES[curTableName];
    var newSelection = document.createElement('select');
    newSelection.setAttribute('id', oldSelection.id)
    newSelection.setAttribute('class', oldSelection.className)
    newSelection.setAttribute('name', oldSelection.name)
    var newOptions = options.forEach(option => {
      var newOption = document.createElement('option');
      newOption.setAttribute('value', option[0]);
      newOption.textContent = option[1];
      newSelection.appendChild(newOption);
    });
    columnParent.replaceChild(newSelection, oldSelection);
  }

  var datasetBox = document.querySelector('#ds-select');
  datasetBox.addEventListener('change', replaceSelection);
})()