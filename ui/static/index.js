var currentTab = 0;

function showTab(n) {
    // This function will display the specified tab of the form...
    var x = document.getElementsByClassName("tab");
    x[n].style.display = "block";
    //... and fix the Previous/Next buttons:
    if (n == 0) {
        document.getElementById("prevBtn").style.display = "none";
    } else {
        document.getElementById("prevBtn").style.display = "inline";
    }
    if (n == (x.length - 2)) {
        document.getElementById("nextBtn").innerHTML = "Submit";
    } else {
        document.getElementById("nextBtn").innerHTML = "Next";
    }
    //... and run a function that will display the correct step indicator:
    fixStepIndicator(n)
}

function nextPrev(n) {
    // This function will figure out which tab to display
    var x = document.getElementsByClassName("tab");
    // Exit the function if any field in the current tab is invalid:
    //if (n == 1 && !validateForm()) return false;
    // Hide the current tab:
    x[currentTab].style.display = "none";
    // Increase or decrease the current tab by 1:
    currentTab = currentTab + n;
    // if you have reached the end of the form...
    if (currentTab >= x.length - 1) {
        // before submitting enable the fragmentation select to POST its value
        fragmentation.removeAttribute("disabled");
        // show loading tab
        showTab(currentTab);
        // ... the form gets submitted:
        document.getElementById("regForm").submit();
        return false;
    }
    if(currentTab == 3){
      generalizatiotTabShow()
    }
    // Otherwise, display the correct tab:
    showTab(currentTab);
}

// TODO: make the validation work with "hidden" input
/*function validateForm() {
    // This function deals with validation of the form fields
    var x, y, i, valid = true;
    x = document.getElementsByClassName("tab");
    y = x[currentTab].getElementsByTagName("input");
    // A loop that checks every input field in the current tab:
    for (i = 0; i < y.length; i++) {
        // If a field is empty...
        if (y[i].value == "") { //
            // add an "invalid" class to the field:
            y[i].className += " invalid";
            // and set the current valid status to false
            valid = false;
        }
    }
    // If the valid status is true, mark the step as finished and valid:
    if (valid) {
        document.getElementsByClassName("step")[currentTab].className += " finish";
    }
    return valid; // return the valid status
}*/

function fixStepIndicator(n) {
    // This function removes the "active" class of all steps...
    var i, x = document.getElementsByClassName("step");
    for (i = 0; i < x.length; i++) {
        x[i].className = x[i].className.replace(" active", "");
    }
    //... and adds the "active" class on the current step:
    if(n < document.getElementsByClassName("step").length){
      x[n].className += " active";
    }else{
      var element = document.getElementById('prevBtn');
      element.parentNode.removeChild(element);
      element = document.getElementById('nextBtn');
      element.parentNode.removeChild(element);
    }
}

function hide(cls) {
    var divs = document.getElementsByClassName(cls);
    for (var i = 0; i < divs.length; i++) {
        divs[i].style.display = "none";
    }
}

function show(cls) {
    var divs = document.getElementsByClassName(cls);
    for (var i = 0; i < divs.length; i++) {
        divs[i].style.display = "block";
    }
}

function checkFractionValue(){
  var fractionElem = document.getElementById('fractionInput');
  var fraction = fractionElem.value
  var fragmentation = document.getElementById('fragmentation')
  if (fraction < 1){
    fragmentation.value = 'quantile'
    fragmentation.setAttribute("disabled", true);
  }else{

    if (fraction > 1 || fraction < 0){
      fractionElem.value = "A number between 0 and 1..."
    }else{
      fragmentation.removeAttribute("disabled");
    }
  }
}

function toggle(cls) {
    var divs = document.getElementsByClassName(cls);
    for (var i = 0; i < divs.length; i++)
        if (divs[i].style.display == "" || divs[i].style.display == "none")
            divs[i].style.display = "block";
        else
            divs[i].style.display = "none";
}

function extractColumns(evt){
  cleanColumnsSelect()
  var files = evt.target.files;
       var file = files[0];
       var reader = new FileReader();
       reader.onload = function(event) {
         var allText = event.target.result;
         var allTextLines = allText.split(/\r\n|\n/);
         var entries = allTextLines[0].split(',')

         for(var i = 0; i < entries.length;i++){
           var node = document.createElement("option")
           var text = document.createTextNode(entries[i]);
           node.setAttribute('value', entries[i])
           node.appendChild(text)
           document.getElementById("identifiers").appendChild(node.cloneNode(true))
           document.getElementById("quasi-identifiers").appendChild(node.cloneNode(true))
           document.getElementById("sensitive").appendChild(node)
         }


       }
       reader.readAsText(file)
}

function cleanColumnsSelect(){
  var toRemove = ["identifiers", "quasi-identifiers","sensitive"]
  for (var i = 0; i < toRemove.length;i++){
    document.getElementById(toRemove[i]).querySelectorAll('*').forEach(n => n.remove());
  }

}

function generalizatiotTabShow(){
  qi = document.getElementById("quasi-identifiers").options
  cleanGeneralizations()
  for (var i = 0; i < qi.length;i++){
    if (qi[i].selected)
    createGeneralizationDiv(qi[i].value)
  }
}

function cleanGeneralizations(){
  document.getElementById("taxTab").querySelectorAll('*').forEach(n => {
    if(n.tagName !== "H3" )
      n.remove()
  });
}

function createGeneralizationDiv(column){

  var select = document.createElement("select")
  var label = document.createElement("label")

  label.setAttribute("for", "quasiid_type_" + column)
  label.appendChild(document.createTextNode('Generalization of ' + column))

  select.setAttribute("id", "quasiid_type_" + column)
  select.setAttribute("name", "quasiid_type_" + column)
  select.setAttribute("oninput", "this.className = ''")

  var option = document.createElement("option")
  option.setAttribute("value", "default")
  option.appendChild(document.createTextNode('Default'))

  select.appendChild(option)

  option = document.createElement("option")
  option.setAttribute("value", "categorical")
  option.appendChild(document.createTextNode('Categorical'))

  select.appendChild(option)

  option = document.createElement("option")
  option.setAttribute("value", "numerical")
  option.appendChild(document.createTextNode('Numerical'))

  select.appendChild(option)

  option = document.createElement("option")
  option.setAttribute("value", "common_prefix")
  option.appendChild(document.createTextNode('Commmon prefix'))

  select.appendChild(option)

  var paragraph = document.createElement("p")
  paragraph.appendChild(select)

  var div = document.createElement("div")
  div.setAttribute("style", "border: 1px solid black;padding: 20px")
  div.setAttribute("id", "quasiid_div_" + column)

  div.appendChild(label)
  div.appendChild(paragraph)

  var categorical_label = document.createElement("label")
  categorical_label.setAttribute("for", "taxonomy_tree_" + column)
  categorical_label.appendChild(document.createTextNode('Taxonomy file for ' + column))

  var file_taxonomy = document.createElement("input")
  file_taxonomy.setAttribute("type", "file")
  file_taxonomy.setAttribute("id", "taxonomy_tree_" + column)
  file_taxonomy.setAttribute("oninput", "this.className = ''")
  file_taxonomy.setAttribute("name", "taxonomy_tree_" + column)
  file_taxonomy.setAttribute("enctype", "multipart/form-data")

  var file_div = document.createElement("div")
  file_div.setAttribute("id", "categorical_div_" + column)
  file_div.setAttribute("style", "display: none")
  file_div.appendChild(categorical_label)
  file_div.appendChild(document.createElement("br"))
  file_div.appendChild(file_taxonomy)
  div.appendChild(file_div)


  var fanout_label = document.createElement("label")
  fanout_label.setAttribute("for", "fanout_" + column)
  fanout_label.appendChild(document.createTextNode('Fanout taxonomy tree for ' + column))
  var fanout = document.createElement("input")
  fanout.setAttribute("type", "number")
  fanout.setAttribute("id", "fanout_" + column)
  fanout.setAttribute("oninput", "this.className = ''")
  fanout.setAttribute("name", "fanout_" + column)

  var accuracy_label = document.createElement("label")
  accuracy_label.setAttribute("for", "accuracy_" + column)
  accuracy_label.appendChild(document.createTextNode('Accuracy for ' + column))
  var accuracy = document.createElement("input")
  accuracy.setAttribute("type", "number")
  accuracy.setAttribute("id", "accuracy_" + column)
  accuracy.setAttribute("oninput", "this.className = ''")
  accuracy.setAttribute("name", "accuracy_" + column)

  var digits_label = document.createElement("label")
  digits_label.setAttribute("for", "digits_" + column)
  digits_label.appendChild(document.createTextNode('Digits for ' + column))
  var digits = document.createElement("input")
  digits.setAttribute("type", "number")
  digits.setAttribute("id", "digits_" + column)
  digits.setAttribute("oninput", "this.className = ''")
  digits.setAttribute("name", "digits_" + column)

  var numerical_div = document.createElement("div")
  numerical_div.setAttribute("id", "numerical_div_" + column)
  numerical_div.setAttribute("style", "display: none")
  numerical_div.appendChild(fanout_label)
  numerical_div.appendChild(document.createElement("br"))
  numerical_div.appendChild(fanout)
  numerical_div.appendChild(accuracy_label)
  numerical_div.appendChild(document.createElement("br"))
  numerical_div.appendChild(accuracy)
  numerical_div.appendChild(digits_label)
  numerical_div.appendChild(document.createElement("br"))
  numerical_div.appendChild(digits)

  div.appendChild(numerical_div)

  var hide_label = document.createElement("label")
  hide_label.setAttribute("for", "hidemark_" + column)
  hide_label.appendChild(document.createTextNode('Hidemark character for ' + column))

  var hidemark = document.createElement("input")
  hidemark.setAttribute("id", "hidemark_" + column)
  hidemark.setAttribute("oninput", "this.className = ''")
  hidemark.setAttribute("name", "hidemark_" + column)

  var hide_div = document.createElement("div")
  hide_div.setAttribute("id", "prefix_div_" + column)
  hide_div.setAttribute("style", "display: none")
  hide_div.appendChild(hide_label)
  hide_div.appendChild(document.createElement("br"))
  hide_div.appendChild(hidemark)
  div.appendChild(hide_div)

  document.getElementById("taxTab").appendChild(div)
  document.getElementById("quasiid_type_" + column).addEventListener('change', showParams, false)
  document.getElementById("taxTab").appendChild(document.createElement("br"))

}

function showParams(event){

  tokens = event.target.id.split('_')

  qi = tokens[tokens.length-1]
  selection = document.getElementById("quasiid_type_" + qi).value

  if(selection === 'default'){
    document.getElementById('numerical_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('prefix_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('categorical_div_' + qi).setAttribute('style', 'display: none')
  }
  if(selection === 'categorical'){
    document.getElementById('numerical_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('prefix_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('categorical_div_' + qi).setAttribute('style', 'display: block')

  }
  if(selection === 'numerical'){
    document.getElementById('categorical_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('prefix_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('numerical_div_' + qi).setAttribute('style', 'display: block')
  }
  if(selection === 'common_prefix'){
    document.getElementById('categorical_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('numerical_div_' + qi).setAttribute('style', 'display: none')
    document.getElementById('prefix_div_' + qi).setAttribute('style', 'display: block')
  }

}
