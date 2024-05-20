use crate::core::{Page, Tid, Tree};
use std::path::{Path};
use std::env;
use std::fs::{File};
use std::io::Write;
use handlebars::*;
use serde_json::{from_str, json};
use serde_json::Value::Object;
use serde_json::value::Value;


struct ChildLevelPagesHelper;

struct IsArrayHelper;

struct IsFirstPageLeafHelper;

struct TidRenderHelper;

struct IsLeafHelper;

impl HelperDef for IsLeafHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();
        let page: Page = serde_json::from_value(param.value().clone()).unwrap();
        if page.is_leaf {
            out.write("true").unwrap();
        } else {
            out.write("false").unwrap();
        }
        Ok(())
    }
}

// Todo: Try setting context instead of writing to output
impl HelperDef for ChildLevelPagesHelper {
    fn call<'reg: 'rc, 'rc>(&self,
                            h: &Helper,
                            _: &Handlebars,
                            ctx: &Context,
                            rc: &mut RenderContext,
                            out: &mut dyn Output) -> HelperResult {
        let param = h.param(0).unwrap();
        println!("child_level_pages path - {:?}", param.relative_path());
        let page = param.value();
        let base_vec: Vec<Value>;
        if page.as_array().is_none() {
            base_vec = vec![page.clone()];
        } else {
            base_vec = page.as_array().unwrap().to_vec();
        }
        let pages: Vec<Page> = base_vec.iter()
            .map(|x| serde_json::from_value(x.clone()).unwrap()).collect();
        let child_pages = child_level_pages(pages);
        println!("child_pages - {:?}", child_pages.len());
        let pages_json = json!(child_pages);
        out.write(pages_json.to_string().as_ref())?;
        let mut updated_ctx = ctx.data();
        if let Object(m) = updated_ctx {
            let mut new_ctx_data = m.clone();
            new_ctx_data.insert("pages".to_string(), pages_json);
            let json_obj = Object(new_ctx_data);
            rc.set_context(Context::wraps(json_obj)?);
        }
        Ok(())
    }
}


impl HelperDef for IsArrayHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();

        // Check if the parameter is an array
        let is_array = match param.value() {
            JsonValue::Array(_) => true,
            _ => false,
        };

        if is_array {
            out.write("true").unwrap()
        } else {
            out.write("false").unwrap()
        }

        Ok(())
    }
}

impl HelperDef for IsFirstPageLeafHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();
        // println!("is_leaf param - {:?}", param);
        let page: Vec<Page> = serde_json::from_value(param.value().clone()).unwrap();
        if page.get(0).unwrap().is_leaf {
            out.write("true").unwrap();
        } else {
            out.write("false").unwrap();
        }

        Ok(())
    }
}

impl HelperDef for TidRenderHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();
        println!("tid param - {:?}", param);
        let tid: Tid = serde_json::from_value(param.value().clone()).unwrap();
        let val = format!("({}, {})", tid.block_number, tid.offset_number);
        out.write(val.as_str()).unwrap();
        Ok(())
    }
}

// Implement your IsString helper
struct IsStringHelper;

impl HelperDef for IsStringHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();

        // Check if the parameter is a string
        let is_string = match param.value() {
            JsonValue::String(_) => true,
            _ => false,
        };

        if is_string {
            out.write("true").unwrap();
        } else {
            out.write("false").unwrap();
        }

        Ok(())
    }
}

fn child_level_pages(pages: Vec<Page>) -> Vec<Page> {
    let mut child_pages: Vec<Page> = vec![];
    for page in pages {
        if page.is_leaf {
            break;
        }
        for item in page.items {
            if let Some(child) = item.child {
                child_pages.push(*child);
            }
        }
    }
    child_pages
}

pub fn render(tree: Tree, output_path: &Path) {
    let mut templates_dir = env::current_dir().unwrap();
    templates_dir = templates_dir.join("src").join("templates");
    let mut handlebars = Handlebars::new();


    let render_tree_file = templates_dir.join("render_tree.hbs");
    let render_page_file = templates_dir.join("render_page.hbs");
    let render_level_file = templates_dir.join("render_level.hbs");
    handlebars.register_template_file("render_tree", render_tree_file).unwrap();
    handlebars.register_template_file("render_page", render_page_file).unwrap();
    handlebars.register_template_file("render_level", render_level_file).unwrap();
    handlebars.register_helper("child_level_pages", Box::new(ChildLevelPagesHelper));
    handlebars.register_helper("isArray", Box::new(IsArrayHelper));
    handlebars.register_helper("isString", Box::new(IsStringHelper));
    handlebars.register_helper("isFirstPageLeaf", Box::new(IsFirstPageLeafHelper));
    handlebars.register_helper("renderTid", Box::new(TidRenderHelper));
    handlebars.register_helper("isLeaf", Box::new(IsLeafHelper));

    let mut map = serde_json::Map::new();
    map.insert("tree".to_string(), serde_json::to_value(&tree).unwrap());
    let child_level_pages = child_level_pages(vec![tree.root.clone()]);
    map.insert("child_level_pages".to_string(), serde_json::to_value(&child_level_pages).unwrap());
    map.insert("index_type".to_string(), serde_json::to_value(&tree.index_type.unwrap()).unwrap());
    let rendered = handlebars.render("render_tree", &map).unwrap();
    let mut file = File::create(output_path).expect("Unable to create file");
    file.write_all(rendered.as_bytes()).expect("Unable to write data to file");
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use crate::core::{Page, Tid};
    use crate::core::structs::Item;

    #[test]
    pub fn test_string_to_page_conversion() {
        let original_page = Page {
            id: 0,
            level: 0,
            is_leaf: false,
            is_root: false,
            items: vec![Item {
                value: "abc".to_string(),
                child: None,
                pointer: None,
                obj_id: Some(Tid {
                    block_number: 1,
                    offset_number: 2,
                }),
            }],
            prev_page_id: Some(1),
            next_page_id: Some(1),
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let v = vec![original_page];
        let json_str = json!(v).to_string();
        println!("{:?}", json_str);
        let page: Vec<Page> = serde_json::from_str(json_str.as_str()).unwrap();
        println!("{:?}", page);
    }
}